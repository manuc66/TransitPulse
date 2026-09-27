//! Ingestion du GTFS statique (horaires théoriques) vers SQLite.
//!
//! Source MVP : TEC Wallonie. Le ZIP officiel (~85 Mo) est mis en cache sous
//! `data/raw/gtfs/` ; ses CSV sont lus en streaming (jamais tout en RAM) et
//! chargés dans `data/tec.sqlite` (`shapes.txt` est ignoré).
//!
//! Le schéma SQLite et la lecture associée (dépôt `GtfsRepo`) vivent ici.
//!
//! Deux garde-fous gardent la base lisible et son poids stable :
//!
//! - le chargement se fait dans un fichier jetable (`tec.sqlite.new`) puis est
//!   renommé sur la cible. Un ETL interrompu laisse donc l'ancienne base
//!   intacte au lieu d'une base à moitié écrite — `journal_mode=OFF`, rendu
//!   nécessaire par la vitesse sur 5,6 M de lignes, interdit tout rollback ;
//! - l'empreinte du ZIP source est mémorisée dans `gtfs_meta`. Au démarrage
//!   suivant, un feed inchangé réutilise la base telle quelle au lieu de
//!   réécrire ~1 Go pendant une vingtaine de secondes.

use std::io::{BufRead, BufReader, Read, Seek};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params};

use crate::domain::{Line, Stop, TransportMode};

/// GTFS statique officiel TEC (Wallonie) via Belgian Mobility.
pub const TEC_GTFS_URL: &str = "https://opendata-discovery-gtfs-static.api.production.belgianmobility.io/api/gtfs/feed/tec/static";
/// Repli (portail opendata de l'opérateur).
pub const TEC_GTFS_URL_FALLBACK: &str = "https://opendata.tec-wl.be/Current%20GTFS/TEC-GTFS.zip";

/// Fraîcheur par défaut du ZIP en cache, en jours.
pub const DEFAULT_GTFS_MAX_AGE_DAYS: u64 = 7;

/// Version du schéma GTFS embarqué, écrite dans `gtfs_meta`.
///
/// À incrémenter dès que `SCHEMA` change : une base dont la version ne
/// correspond plus est considérée incompatible et reconstruite, au lieu de
/// laisser `GtfsRepo` lire des colonnes absentes.
const SCHEMA_VERSION: &str = "1";

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS gtfs_stops (
    stop_id       TEXT PRIMARY KEY,
    stop_name     TEXT NOT NULL,
    stop_desc     TEXT,
    lat           REAL NOT NULL,
    lon           REAL NOT NULL,
    wheelchair    INTEGER,
    zone_id       TEXT,
    parent_station TEXT
);
CREATE INDEX IF NOT EXISTS idx_stops_name ON gtfs_stops(stop_name);

CREATE TABLE IF NOT EXISTS gtfs_routes (
    route_id    TEXT PRIMARY KEY,
    short_name  TEXT,
    long_name   TEXT,
    route_type  INTEGER NOT NULL,
    color       TEXT
);

CREATE TABLE IF NOT EXISTS gtfs_trips (
    trip_id     TEXT PRIMARY KEY,
    route_id    TEXT NOT NULL,
    service_id  TEXT NOT NULL,
    headsign    TEXT,
    direction   INTEGER
);
CREATE INDEX IF NOT EXISTS idx_trips_route   ON gtfs_trips(route_id);
CREATE INDEX IF NOT EXISTS idx_trips_service ON gtfs_trips(service_id);

CREATE TABLE IF NOT EXISTS gtfs_stop_times (
    trip_id         TEXT NOT NULL,
    stop_sequence   INTEGER NOT NULL,
    stop_id         TEXT NOT NULL,
    arrival_secs    INTEGER,
    departure_secs  INTEGER,
    PRIMARY KEY (trip_id, stop_sequence)
);
-- Index sur gtfs_stop_times : voir `idx_st_idx_stop`, créé après le chargement.
-- Il est volontairement ABSENT du schéma : maintenir un index pendant
-- l'insertion des 5,6 M de lignes coûte ~40 % du temps de l'ETL.
-- trip_id n'a pas besoin d'index : c'est la première colonne du PRIMARY KEY,
-- qui sert déjà les jointures par course.

CREATE TABLE IF NOT EXISTS gtfs_calendar (
    service_id  TEXT PRIMARY KEY,
    monday      INTEGER NOT NULL,
    tuesday     INTEGER NOT NULL,
    wednesday   INTEGER NOT NULL,
    thursday    INTEGER NOT NULL,
    friday      INTEGER NOT NULL,
    saturday    INTEGER NOT NULL,
    sunday      INTEGER NOT NULL,
    start_date  TEXT NOT NULL,
    end_date    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS gtfs_calendar_dates (
    service_id      TEXT NOT NULL,
    date            TEXT NOT NULL,
    exception_type  INTEGER NOT NULL,
    PRIMARY KEY (service_id, date)
);

CREATE TABLE IF NOT EXISTS gtfs_meta (key TEXT PRIMARY KEY, value TEXT);
"#;

/// Convertit un horaire GTFS `HH:MM:SS` en secondes depuis minuit.
/// Les heures peuvent dépasser 24 (courses de nuit) : on ne les rejette pas.
pub fn parse_gtfs_time(s: &str) -> Option<i64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let mut it = s.split(':');
    let h: i64 = it.next()?.parse().ok()?;
    let m: i64 = it.next()?.parse().ok()?;
    let sec: i64 = it.next()?.parse().ok()?;
    Some(h * 3600 + m * 60 + sec)
}

/// Convertit `route_type` GTFS en mode applicatif.
pub fn route_type_to_mode(t: i64) -> TransportMode {
    match t {
        0 | 5 | 6 | 7 | 11 | 12 => TransportMode::Tram,
        1 => TransportMode::Metro,
        2 => TransportMode::Train,
        _ => TransportMode::Bus,
    }
}

/// Écrit les lignes de `calendar.txt` dans l'ordre des colonnes.
fn calendar_columns() -> [&'static str; 10] {
    [
        "service_id",
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
        "start_date",
        "end_date",
    ]
}

/// Télécharge le ZIP GTFS sous `cache_path` s'il est absent ou périmé, puis le
/// charge dans SQLite — sauf si la base existante décrit déjà ce ZIP.
///
/// `max_age_days` borne la fraîcheur du ZIP en cache : sans cela, le « on ne
/// recharge que si l'empreinte change » gèlerait le réseau sur le premier
/// téléchargement.
///
/// Un rafraîchissement qui échoue n'est pas fatal tant qu'un cache utilisable
/// reste sur disque : perdre le réseau un jour de péremption ne doit pas faire
/// tomber l'application sur le jeu fictif alors que `tec.sqlite` est complet.
pub async fn fetch_and_load(
    client: &reqwest::Client,
    url: &str,
    cache_path: &Path,
    db_path: &Path,
    max_age_days: u64,
) -> Result<GtfsLoad> {
    let zip_age = zip_age_days(cache_path);
    if zip_age.is_none_or(|age| age >= max_age_days) {
        tracing::info!(
            age_days = zip_age,
            max_age_days,
            "ZIP GTFS absent ou périmé — téléchargement"
        );
        if let Err(e) = download_with_fallback(client, url, cache_path).await {
            if cache_path.exists() {
                tracing::warn!(error = %e, "rafraîchissement impossible — cache conservé");
            } else {
                return Err(e);
            }
        }
    }
    let fingerprint = zip_fingerprint(cache_path)?;

    if is_reusable(db_path, &fingerprint)? {
        tracing::info!(path = %db_path.display(), "GTFS déjà chargé — ETL sauté");
        return Ok(GtfsLoad::Reused);
    }

    // Le parsing est CPU/IO lourds : on le sort du runtime async.
    let cache = cache_path.to_path_buf();
    let db = db_path.to_path_buf();
    let fp = fingerprint.clone();
    let started = std::time::Instant::now();
    let stats =
        tokio::task::spawn_blocking(move || load_zip_into_sqlite(&cache, &db, &fp)).await??;
    tracing::info!(elapsed_s = started.elapsed().as_secs(), "GTFS rechargé");
    Ok(GtfsLoad::Loaded { stats })
}

/// Résultat d'un appel à [`fetch_and_load`].
#[derive(Debug, Clone, Copy)]
pub enum GtfsLoad {
    /// La base existante décrit déjà le ZIP en cache : rien n'a été écrit.
    Reused,
    /// ZIP éventuellement rafraîchi, puis base reconstruite.
    Loaded { stats: GtfsStats },
}

/// Ancienneté du ZIP en cache, en jours entiers. `None` s'il n'existe pas.
fn zip_age_days(zip_path: &Path) -> Option<u64> {
    let mtime = std::fs::metadata(zip_path).ok()?.modified().ok()?;
    let age = std::time::SystemTime::now()
        .duration_since(mtime)
        .ok()?
        .as_secs();
    Some(age / 86_400)
}

/// Empreinte du ZIP source : taille + date de modification, pas son contenu.
///
/// Volontairement O(1) : elle est lue à chaque démarrage, et le but est
/// justement d'éviter de relire 85 Mo. Une collision demanderait deux ZIP de
/// même taille posés à la même seconde.
fn zip_fingerprint(zip_path: &Path) -> Result<String> {
    let md = std::fs::metadata(zip_path).with_context(|| format!("stat {}", zip_path.display()))?;
    let mtime = md
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    Ok(format!("{}:{mtime}", md.len()))
}

/// Lit une clé de `gtfs_meta`. `None` si absente, table absente ou base illisible.
fn read_meta(conn: &Connection, key: &str) -> Option<String> {
    conn.query_row(
        "SELECT value FROM gtfs_meta WHERE key = ?1",
        params![key],
        |r| r.get(0),
    )
    .optional()
    .ok()
    .flatten()
}

fn write_meta(conn: &Connection, pairs: &[(&str, &str)]) -> Result<()> {
    let tx = conn.unchecked_transaction()?;
    for (k, v) in pairs {
        tx.execute(
            "INSERT OR REPLACE INTO gtfs_meta (key, value) VALUES (?1, ?2)",
            params![k, v],
        )?;
    }
    tx.commit()?;
    Ok(())
}

/// La base existante peut-elle être réutilisée telle quelle ?
///
/// Trois conditions, toutes nécessaires :
///
/// - le schéma embarqué correspond à celui qui a écrit la base, sinon les
///   colonnes lues par `GtfsRepo` peuvent ne plus exister ;
/// - l'empreinte du ZIP est celle du fichier en cache, donc le réseau n'a pas
///   bougé depuis le dernier chargement ;
/// - les tables sont réellement peuplées. C'est le filet qui rattrape les bases
///   mutilées : ETL interrompu, disque plein, suppression manuelle.
///   `gtfs_stop_times` vide en est le symptôme typique — les trips sont
///   chargés en premier et donnent l'impression que tout va bien.
///
/// Une base qui échoue n'est pas une erreur fatale : elle est reconstruite.
fn is_reusable(db_path: &Path, fingerprint: &str) -> Result<bool> {
    if !db_path.exists() {
        return Ok(false);
    }
    let conn = match Connection::open(db_path) {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(error = %e, "base GTFS illisible — rechargement");
            return Ok(false);
        }
    };
    if read_meta(&conn, "schema_version").as_deref() != Some(SCHEMA_VERSION) {
        return Ok(false);
    }
    if read_meta(&conn, "zip_fingerprint").as_deref() != Some(fingerprint) {
        return Ok(false);
    }
    let count = |t: &str| -> i64 {
        conn.query_row(&format!("SELECT COUNT(*) FROM {t}"), [], |r| r.get(0))
            .unwrap_or(0)
    };
    let (trips, stop_times) = (count("gtfs_trips"), count("gtfs_stop_times"));
    if trips == 0 || stop_times == 0 {
        tracing::warn!(trips, stop_times, "base GTFS incomplète — rechargement");
        return Ok(false);
    }
    Ok(true)
}

/// Fichier de construction jetable, à côté de la base cible (`tec.sqlite.new`).
fn staging_path(db_path: &Path) -> PathBuf {
    let mut s = db_path.as_os_str().to_os_string();
    s.push(".new");
    PathBuf::from(s)
}

/// Télécharge le ZIP, en essayant `url` puis `TEC_GTFS_URL_FALLBACK`.
async fn download_with_fallback(
    client: &reqwest::Client,
    url: &str,
    cache_path: &Path,
) -> Result<usize> {
    let urls = [url, TEC_GTFS_URL_FALLBACK];
    let mut last_err = None;
    for (i, u) in urls.iter().enumerate() {
        tracing::info!(
            url = u,
            tentative = i + 1,
            "téléchargement du GTFS statique"
        );
        match client.get(*u).send().await {
            Ok(resp) => match resp.error_for_status() {
                Ok(resp) => match resp.bytes().await {
                    Ok(bytes) if !bytes.is_empty() => {
                        std::fs::write(cache_path, &bytes)?;
                        return Ok(bytes.len());
                    }
                    Ok(_) => last_err = Some(anyhow::anyhow!("réponse GTFS vide ({u})")),
                    Err(e) => last_err = Some(anyhow::Error::from(e)),
                },
                Err(e) => last_err = Some(anyhow::Error::from(e)),
            },
            Err(e) => last_err = Some(anyhow::Error::from(e)),
        }
        if i + 1 < urls.len() {
            tracing::warn!(url = u, error = ?last_err, "échec, repli sur la source suivante");
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("aucune source GTFS disponible")))
}

#[derive(Debug, Clone, Copy, Default)]
pub struct GtfsStats {
    pub stops: usize,
    pub routes: usize,
    pub trips: usize,
    pub stop_times: usize,
    pub calendar: usize,
    pub calendar_dates: usize,
}

/// Charge un ZIP GTFS dans `db_path`, en le remplaçant atomiquement.
///
/// Le travail se fait dans `tec.sqlite.new`, un fichier jetable renommé sur la
/// cible une fois complet. Deux bénéfices :
///
/// - `journal_mode=OFF` (justifié par les 5,6 M de lignes) devient sans risque,
///   puisque la base jetable est abandonnée en cas d'échec ou d'interruption ;
/// - la base cible n'est plus `DELETE`-puis-remplie, elle ne conserve donc pas
///   de pages libérées en fin de fichier. Un `VACUUM` serait alors inutile.
///
/// `fingerprint` est mémorisé dans `gtfs_meta` une fois la base complète : c'est
/// ce qui permet à [`is_reusable`] de sauter l'ETL au démarrage suivant.
pub fn load_zip_into_sqlite(
    zip_path: &Path,
    db_path: &Path,
    fingerprint: &str,
) -> Result<GtfsStats> {
    let file = std::fs::File::open(zip_path)
        .with_context(|| format!("ouverture {}", zip_path.display()))?;
    let mut zip = zip::ZipArchive::new(file).context("lecture ZIP GTFS")?;

    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let staging = staging_path(db_path);
    // Un reliquat d'un run interrompu ferait échouer la publication finale.
    let _ = std::fs::remove_file(&staging);

    match build_sqlite(&mut zip, &staging, fingerprint) {
        Ok(stats) => {
            std::fs::rename(&staging, db_path).with_context(|| {
                format!(
                    "publication de {} vers {}",
                    staging.display(),
                    db_path.display()
                )
            })?;
            // La base remplacée ne peut plus avoir de WAL : ces fichiers
            // appartenaient à l'ancienne et iraient à sa suite.
            for suffix in ["-wal", "-shm", "-journal"] {
                let mut s = db_path.as_os_str().to_os_string();
                s.push(suffix);
                let _ = std::fs::remove_file(PathBuf::from(s));
            }
            Ok(stats)
        }
        Err(e) => {
            // La cible n'a pas été touchée : on ne garde que le déchet.
            let _ = std::fs::remove_file(&staging);
            Err(e)
        }
    }
}

/// Remplit une base SQLite neuve à partir des CSV du ZIP.
fn build_sqlite<R: Read + Seek>(
    zip: &mut zip::ZipArchive<R>,
    db_path: &Path,
    fingerprint: &str,
) -> Result<GtfsStats> {
    let conn = Connection::open(db_path)?;
    conn.pragma_update(None, "journal_mode", "OFF")?;
    conn.pragma_update(None, "synchronous", "OFF")?;
    conn.execute_batch(SCHEMA)?;

    // Base neuve : aucune table à vider, aucun index à retirer.
    let mut stats = GtfsStats::default();
    {
        let tx = conn.unchecked_transaction()?;
        with_reader(zip, "stops.txt", |r| load_stops(&tx, r, &mut stats))?;
        with_reader(zip, "routes.txt", |r| load_routes(&tx, r, &mut stats))?;
        with_reader(zip, "trips.txt", |r| load_trips(&tx, r, &mut stats))?;
        with_reader(zip, "stop_times.txt", |r| {
            load_stop_times(&tx, r, &mut stats)
        })?;
        with_reader(zip, "calendar.txt", |r| load_calendar(&tx, r, &mut stats))?;
        with_reader(zip, "calendar_dates.txt", |r| {
            load_calendar_dates(&tx, r, &mut stats)
        })?;
        tx.commit()?;
    }
    // Index créé après l'insertion : maintenir un index ligne par ligne pendant
    // le chargement des 5,6 M de stop_times coûte ~40 % du temps de l'ETL.
    // Mesuré sur le ZIP TEC complet : 24,1 s -> 15,1 s, et 1,30 Go -> 0,96 Go
    // de base (l'index sur trip_id, redondant avec le PRIMARY KEY, pesait
    // pour ~340 Mo à lui seul).
    conn.execute_batch("CREATE INDEX IF NOT EXISTS idx_st_idx_stop ON gtfs_stop_times(stop_id);")?;
    conn.execute_batch("ANALYZE;")?;
    // L'empreinte n'est écrite qu'ici, après le dernier INSERT : c'est elle
    // qui autorise `is_reusable` à sauter l'ETL au démarrage suivant.
    write_meta(
        &conn,
        &[
            ("schema_version", SCHEMA_VERSION),
            ("zip_fingerprint", fingerprint),
            ("loaded_at", &chrono::Utc::now().to_rfc3339()),
            ("stops", &stats.stops.to_string()),
            ("routes", &stats.routes.to_string()),
            ("trips", &stats.trips.to_string()),
            ("stop_times", &stats.stop_times.to_string()),
        ],
    )?;
    Ok(stats)
}

/// Ouvre le fichier `name` du ZIP et le passe à `f` en streaming ligne par ligne.
fn with_reader<S, F>(zip: &mut zip::ZipArchive<S>, name: &str, f: F) -> Result<()>
where
    S: Read + Seek,
    F: FnOnce(&mut dyn BufRead) -> Result<()>,
{
    let entry = zip
        .by_name(name)
        .with_context(|| format!("entrée {name} absente du ZIP"))?;
    let mut reader = BufReader::with_capacity(1 << 20, entry);
    f(&mut reader)
}

fn header_index(headers: &csv::StringRecord, col: &str) -> Result<usize> {
    headers
        .iter()
        .position(|h| h == col)
        .with_context(|| format!("colonne {col} absente"))
}

fn load_stops(tx: &Connection, reader: &mut dyn BufRead, stats: &mut GtfsStats) -> Result<()> {
    let mut rdr = csv::ReaderBuilder::new().from_reader(reader);
    let headers = rdr.headers()?.clone();
    let idx = |c: &str| header_index(&headers, c);
    let (i_id, i_name, i_desc, i_lat, i_lon) = (
        idx("stop_id")?,
        idx("stop_name")?,
        idx("stop_desc").ok(),
        idx("stop_lat")?,
        idx("stop_lon")?,
    );
    let i_wheel = idx("wheelchair_boarding").ok();
    let i_zone = idx("zone_id").ok();
    let i_parent = idx("parent_station").ok();

    let mut stmt = tx.prepare(
        "INSERT OR REPLACE INTO gtfs_stops
         (stop_id, stop_name, stop_desc, lat, lon, wheelchair, zone_id, parent_station)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
    )?;
    for rec in rdr.records() {
        let rec = rec?;
        let lat: f64 = rec.get(i_lat).unwrap_or("0").parse().unwrap_or(0.0);
        let lon: f64 = rec.get(i_lon).unwrap_or("0").parse().unwrap_or(0.0);
        if lat == 0.0 && lon == 0.0 {
            continue;
        }
        let get_opt = |i: Option<usize>| i.and_then(|i| rec.get(i)).filter(|s| !s.is_empty());
        stmt.execute(params![
            rec.get(i_id).unwrap_or(""),
            rec.get(i_name).unwrap_or(""),
            get_opt(i_desc),
            lat,
            lon,
            get_opt(i_wheel).and_then(|s| s.parse::<i64>().ok()),
            get_opt(i_zone),
            get_opt(i_parent),
        ])?;
        stats.stops += 1;
    }
    Ok(())
}

fn load_routes(tx: &Connection, reader: &mut dyn BufRead, stats: &mut GtfsStats) -> Result<()> {
    let mut rdr = csv::ReaderBuilder::new().from_reader(reader);
    let headers = rdr.headers()?.clone();
    let idx = |c: &str| header_index(&headers, c);
    let (i_id, i_short, i_long, i_type) = (
        idx("route_id")?,
        idx("route_short_name")?,
        idx("route_long_name")?,
        idx("route_type")?,
    );
    let i_color = idx("route_color").ok();

    let mut stmt = tx.prepare(
        "INSERT OR REPLACE INTO gtfs_routes (route_id, short_name, long_name, route_type, color)
         VALUES (?1,?2,?3,?4,?5)",
    )?;
    for rec in rdr.records() {
        let rec = rec?;
        let route_type: i64 = rec.get(i_type).unwrap_or("3").parse().unwrap_or(3);
        stmt.execute(params![
            rec.get(i_id).unwrap_or(""),
            rec.get(i_short).unwrap_or(""),
            rec.get(i_long).unwrap_or(""),
            route_type,
            i_color.and_then(|i| rec.get(i)).filter(|s| !s.is_empty()),
        ])?;
        stats.routes += 1;
    }
    Ok(())
}

fn load_trips(tx: &Connection, reader: &mut dyn BufRead, stats: &mut GtfsStats) -> Result<()> {
    let mut rdr = csv::ReaderBuilder::new().from_reader(reader);
    let headers = rdr.headers()?.clone();
    let idx = |c: &str| header_index(&headers, c);
    let (i_id, i_route, i_service, i_head) = (
        idx("trip_id")?,
        idx("route_id")?,
        idx("service_id")?,
        idx("trip_headsign")?,
    );
    let i_dir = idx("direction_id").ok();

    let mut stmt = tx.prepare(
        "INSERT OR REPLACE INTO gtfs_trips (trip_id, route_id, service_id, headsign, direction)
         VALUES (?1,?2,?3,?4,?5)",
    )?;
    for rec in rdr.records() {
        let rec = rec?;
        stmt.execute(params![
            rec.get(i_id).unwrap_or(""),
            rec.get(i_route).unwrap_or(""),
            rec.get(i_service).unwrap_or(""),
            rec.get(i_head).unwrap_or(""),
            i_dir
                .and_then(|i| rec.get(i))
                .and_then(|s| s.parse::<i64>().ok()),
        ])?;
        stats.trips += 1;
    }
    Ok(())
}

fn load_stop_times(tx: &Connection, reader: &mut dyn BufRead, stats: &mut GtfsStats) -> Result<()> {
    let mut rdr = csv::ReaderBuilder::new().from_reader(reader);
    let headers = rdr.headers()?.clone();
    let idx = |c: &str| header_index(&headers, c);
    let (i_arr, i_dep, i_stop, i_seq, i_trip) = (
        idx("arrival_time")?,
        idx("departure_time")?,
        idx("stop_id")?,
        idx("stop_sequence")?,
        idx("trip_id")?,
    );

    let mut stmt = tx.prepare(
        "INSERT OR REPLACE INTO gtfs_stop_times
         (trip_id, stop_sequence, stop_id, arrival_secs, departure_secs)
         VALUES (?1,?2,?3,?4,?5)",
    )?;
    for rec in rdr.records() {
        let rec = rec?;
        let seq: i64 = match rec.get(i_seq).and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => continue,
        };
        stmt.execute(params![
            rec.get(i_trip).unwrap_or(""),
            seq,
            rec.get(i_stop).unwrap_or(""),
            rec.get(i_arr).and_then(parse_gtfs_time),
            rec.get(i_dep).and_then(parse_gtfs_time),
        ])?;
        stats.stop_times += 1;
        if stats.stop_times.is_multiple_of(1_000_000) {
            tracing::info!(stop_times = stats.stop_times, "stop_times en cours");
        }
    }
    Ok(())
}

fn load_calendar(tx: &Connection, reader: &mut dyn BufRead, stats: &mut GtfsStats) -> Result<()> {
    let mut rdr = csv::ReaderBuilder::new().from_reader(reader);
    let headers = rdr.headers()?.clone();
    let cols = calendar_columns();
    let idx: Vec<usize> = cols
        .iter()
        .map(|c| header_index(&headers, c))
        .collect::<Result<_>>()?;

    let mut stmt = tx.prepare(
        "INSERT OR REPLACE INTO gtfs_calendar
         (service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, start_date, end_date)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
    )?;
    for rec in rdr.records() {
        let rec = rec?;
        let g = |i: usize| rec.get(i).unwrap_or("");
        stmt.execute(params![
            g(idx[0]),
            g(idx[1]).parse::<i64>().unwrap_or(0),
            g(idx[2]).parse::<i64>().unwrap_or(0),
            g(idx[3]).parse::<i64>().unwrap_or(0),
            g(idx[4]).parse::<i64>().unwrap_or(0),
            g(idx[5]).parse::<i64>().unwrap_or(0),
            g(idx[6]).parse::<i64>().unwrap_or(0),
            g(idx[7]).parse::<i64>().unwrap_or(0),
            g(idx[8]),
            g(idx[9]),
        ])?;
        stats.calendar += 1;
    }
    Ok(())
}

fn load_calendar_dates(
    tx: &Connection,
    reader: &mut dyn BufRead,
    stats: &mut GtfsStats,
) -> Result<()> {
    let mut rdr = csv::ReaderBuilder::new().from_reader(reader);
    let headers = rdr.headers()?.clone();
    let idx = |c: &str| header_index(&headers, c);
    let (i_service, i_date, i_type) = (idx("service_id")?, idx("date")?, idx("exception_type")?);

    let mut stmt = tx.prepare(
        "INSERT OR REPLACE INTO gtfs_calendar_dates (service_id, date, exception_type)
         VALUES (?1,?2,?3)",
    )?;
    for rec in rdr.records() {
        let rec = rec?;
        stmt.execute(params![
            rec.get(i_service).unwrap_or(""),
            rec.get(i_date).unwrap_or(""),
            rec.get(i_type).unwrap_or("0").parse::<i64>().unwrap_or(0),
        ])?;
        stats.calendar_dates += 1;
    }
    Ok(())
}

// --- Dépôt de lecture -------------------------------------------------------

/// Accès lecture au GTFS statique indexé.
pub struct GtfsRepo {
    conn: Connection,
}

impl GtfsRepo {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open(path)?;
        Ok(Self { conn })
    }

    pub fn open_in_memory() -> Result<Self> {
        Ok(Self {
            conn: Connection::open_in_memory()?,
        })
    }

    /// Crée le schéma sur une connexion vide (tests).
    #[cfg(test)]
    fn init_schema(&self) -> Result<()> {
        self.conn.execute_batch(SCHEMA)?;
        Ok(())
    }

    /// Crée le schéma GTFS dans une base sur fichier (tests d'intégration).
    #[cfg(test)]
    pub(crate) fn create_schema_at(path: impl AsRef<Path>) -> Result<Connection> {
        let conn = Connection::open(path)?;
        conn.execute_batch(SCHEMA)?;
        Ok(conn)
    }

    /// Insère un arrêt et, si `served`, un service/trip/stop_time le desservant.
    #[cfg(test)]
    fn seed_stop(&self, stop_id: &str, name: &str, lat: f64, lon: f64, served: bool) -> Result<()> {
        self.conn.execute(
            "INSERT INTO gtfs_stops (stop_id, stop_name, lat, lon) VALUES (?1, ?2, ?3, ?4)",
            params![stop_id, name, lat, lon],
        )?;
        if served {
            let route_id = format!("r-{stop_id}");
            let trip_id = format!("t-{stop_id}");
            self.conn.execute(
                "INSERT OR IGNORE INTO gtfs_routes (route_id, short_name, long_name, route_type)
                 VALUES (?1, '1', 'Test', 3)",
                params![route_id],
            )?;
            self.conn.execute(
                "INSERT OR IGNORE INTO gtfs_trips (trip_id, route_id, service_id, headsign)
                 VALUES (?1, ?2, 's1', 'Test')",
                params![trip_id, route_id],
            )?;
            self.conn.execute(
                "INSERT OR IGNORE INTO gtfs_stop_times (trip_id, stop_sequence, stop_id, departure_secs)
                 VALUES (?1, 1, ?2, 3600)",
                params![trip_id, stop_id],
            )?;
        }
        Ok(())
    }

    /// Recherche d'arrêts par nom (sous-chaîne, insensible à la casse).
    ///
    /// Priorise les arrêts réellement desservis (`gtfs_stop_times`) ; les
    /// stations parentes sans horaires passent en dernier.
    pub fn search_stops(&self, q: &str, limit: usize) -> Result<Vec<Stop>> {
        let q = format!("%{}%", q.trim().to_lowercase());
        let mut stmt = self.conn.prepare(
            "SELECT s.stop_id, s.stop_name, s.lat, s.lon
             FROM gtfs_stops s
             LEFT JOIN (SELECT DISTINCT stop_id FROM gtfs_stop_times) st
                    ON st.stop_id = s.stop_id
             WHERE lower(s.stop_name) LIKE ?1
             ORDER BY (st.stop_id IS NULL), s.stop_name
             LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![q, limit as i64], |r| {
            Ok(Stop {
                stop_id: r.get(0)?,
                name: r.get(1)?,
                lat: r.get(2)?,
                lon: r.get(3)?,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    /// Arrêts à proximité d'un point, dans un rayon donné (mètres), triés par
    /// distance croissante. Les arrêts réellement desservis passent en premier.
    pub fn nearby_stops(&self, lat: f64, lon: f64, radius_m: f64) -> Result<Vec<NearbyStop>> {
        // Boîte englobante approx. pour limiter les lignes scannées.
        let dlat = radius_m / 111_320.0;
        let dlon = radius_m / (111_320.0 * lat.to_radians().cos().max(0.01));
        let mut stmt = self.conn.prepare(
            "SELECT s.stop_id, s.stop_name, s.lat, s.lon,
                    EXISTS(SELECT 1 FROM gtfs_stop_times st WHERE st.stop_id = s.stop_id) AS served
             FROM gtfs_stops s
             WHERE s.lat BETWEEN ?1 AND ?2 AND s.lon BETWEEN ?3 AND ?4",
        )?;
        let rows = stmt.query_map(
            params![lat - dlat, lat + dlat, lon - dlon, lon + dlon],
            |r| {
                Ok((
                    Stop {
                        stop_id: r.get(0)?,
                        name: r.get(1)?,
                        lat: r.get(2)?,
                        lon: r.get(3)?,
                    },
                    r.get::<_, i64>(4)? != 0,
                ))
            },
        )?;
        let mut out = Vec::new();
        for row in rows {
            let (stop, served) = row?;
            let metres = crate::geo::haversine_m(lat, lon, stop.lat, stop.lon);
            if metres <= radius_m {
                out.push(NearbyStop {
                    stop,
                    metres,
                    served,
                });
            }
        }
        out.sort_by_key(|n| (!n.served, n.metres as i64));
        out.truncate(20);
        Ok(out)
    }

    /// Arrêt le plus proche d'un point, dans un rayon donné (mètres).
    pub fn nearest_stop(&self, lat: f64, lon: f64, radius_m: f64) -> Result<Option<Stop>> {
        Ok(self
            .nearby_stops(lat, lon, radius_m)?
            .into_iter()
            .next()
            .map(|n| n.stop))
    }

    /// Quais desservis rattachés à une station parente (`parent_station`).
    /// Renvoie aussi la station elle-même.
    pub fn station_stops(&self, stop_id: &str) -> Result<Vec<Stop>> {
        let mut stmt = self.conn.prepare(
            "SELECT stop_id, stop_name, lat, lon FROM gtfs_stops
             WHERE stop_id = ?1 OR parent_station = ?1
             ORDER BY stop_name",
        )?;
        let rows = stmt.query_map(params![stop_id], |r| {
            Ok(Stop {
                stop_id: r.get(0)?,
                name: r.get(1)?,
                lat: r.get(2)?,
                lon: r.get(3)?,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    /// Premier et dernier passage de la journée (secondes depuis minuit) à un
    /// arrêt, pour les services actifs. `None` si l'arrêt n'est pas desservi.
    pub fn day_extent(
        &self,
        stop_ids: &[String],
        services: &[String],
    ) -> Result<Option<(i64, i64)>> {
        if stop_ids.is_empty() || services.is_empty() {
            return Ok(None);
        }
        let stop_ph = (0..stop_ids.len())
            .map(|i| format!("?{}", i + 1))
            .collect::<Vec<_>>()
            .join(",");
        let svc_ph = (0..services.len())
            .map(|i| format!("?{}", i + 1 + stop_ids.len()))
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT MIN(st.departure_secs), MAX(st.departure_secs)
             FROM gtfs_stop_times st
             JOIN gtfs_trips t ON t.trip_id = st.trip_id
             WHERE st.stop_id IN ({stop_ph})
               AND st.departure_secs IS NOT NULL
               AND t.service_id IN ({svc_ph})"
        );
        let mut binds: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
        for s in stop_ids {
            binds.push(Box::new(s.clone()));
        }
        for s in services {
            binds.push(Box::new(s.clone()));
        }
        let refs: Vec<&dyn rusqlite::ToSql> = binds.iter().map(|b| b.as_ref()).collect();
        let row = self
            .conn
            .query_row(&sql, refs.as_slice(), |r| {
                Ok((r.get::<_, Option<i64>>(0)?, r.get::<_, Option<i64>>(1)?))
            })
            .optional()?;
        Ok(row.and_then(|(a, b)| match (a, b) {
            (Some(a), Some(b)) => Some((a, b)),
            _ => None,
        }))
    }

    /// Indique si un arrêt est effectivement desservi (au moins une course).
    pub fn stop_is_served(&self, stop_id: &str) -> Result<bool> {
        let n: i64 = self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM gtfs_stop_times WHERE stop_id = ?1)",
            params![stop_id],
            |r| r.get(0),
        )?;
        Ok(n != 0)
    }

    /// Une ligne par `route_id`.
    pub fn route(&self, route_id: &str) -> Result<Option<Line>> {
        let mut stmt = self.conn.prepare(
            "SELECT route_id, short_name, long_name, route_type FROM gtfs_routes WHERE route_id = ?1",
        )?;
        let mut rows = stmt.query_map(params![route_id], |r| {
            let rt: i64 = r.get(3)?;
            Ok(Line {
                route_id: r.get(0)?,
                short_name: r.get(1)?,
                long_name: r.get(2)?,
                mode: route_type_to_mode(rt),
            })
        })?;
        Ok(rows.next().transpose()?)
    }

    /// Les `service_id` actifs pour une date `YYYYMMDD` (calendrier + exceptions).
    pub fn active_services(&self, yyyymmdd: &str) -> Result<Vec<String>> {
        let weekday_col = crate::gtfs::weekday_column(yyyymmdd)?;
        let sql = format!(
            "SELECT service_id FROM gtfs_calendar
             WHERE {weekday_col} = 1 AND start_date <= ?1 AND end_date >= ?1
             UNION
             SELECT service_id FROM gtfs_calendar
             WHERE service_id IN (
                 SELECT service_id FROM gtfs_calendar_dates
                 WHERE date = ?1 AND exception_type = 1)
             EXCEPT
             SELECT service_id FROM gtfs_calendar_dates
             WHERE date = ?1 AND exception_type = 2"
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(params![yyyymmdd], |r| r.get::<_, String>(0))?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    pub fn stop(&self, stop_id: &str) -> Result<Option<Stop>> {
        let mut stmt = self
            .conn
            .prepare("SELECT stop_id, stop_name, lat, lon FROM gtfs_stops WHERE stop_id = ?1")?;
        let mut rows = stmt.query_map(params![stop_id], |r| {
            Ok(Stop {
                stop_id: r.get(0)?,
                name: r.get(1)?,
                lat: r.get(2)?,
                lon: r.get(3)?,
            })
        })?;
        Ok(rows.next().transpose()?)
    }

    /// Lignes desservant un arrêt (et directions/headsigns associés).
    pub fn lines_for_stop(&self, stop_id: &str) -> Result<Vec<Line>> {
        let mut stmt = self.conn.prepare(
            "SELECT r.route_id, r.short_name, r.long_name, r.route_type
             FROM gtfs_routes r
             JOIN gtfs_trips t ON t.route_id = r.route_id
             JOIN gtfs_stop_times st ON st.trip_id = t.trip_id
             WHERE st.stop_id = ?1
             GROUP BY r.route_id
             ORDER BY r.short_name",
        )?;
        let rows = stmt.query_map(params![stop_id], |r| {
            let rt: i64 = r.get(3)?;
            Ok(Line {
                route_id: r.get(0)?,
                short_name: r.get(1)?,
                long_name: r.get(2)?,
                mode: route_type_to_mode(rt),
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    /// Sens de passage à un arrêt : ligne + destination (headsign) distincts.
    /// Sert à distinguer deux quais de même nom (sens opposés).
    pub fn directions_for_stop(&self, stop_id: &str, limit: usize) -> Result<Vec<Direction>> {
        let mut stmt = self.conn.prepare(
            "SELECT DISTINCT r.short_name, t.headsign, r.route_type
             FROM gtfs_stop_times st
             JOIN gtfs_trips t ON t.trip_id = st.trip_id
             JOIN gtfs_routes r ON r.route_id = t.route_id
             WHERE st.stop_id = ?1
             ORDER BY r.short_name, t.headsign
             LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![stop_id, limit as i64], |r| {
            let rt: i64 = r.get(2)?;
            Ok(Direction {
                short_name: r.get(0)?,
                headsign: r.get(1)?,
                mode: route_type_to_mode(rt),
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    /// Enrichit une liste d'arrêts proches avec leurs destinations, en
    /// écartant les arrêts non desservis (ex. stations parentes vides).
    pub fn to_choices(
        &self,
        nearby: Vec<NearbyStop>,
        keep_unserved: bool,
    ) -> Result<Vec<StopChoice>> {
        let mut out = Vec::new();
        for n in nearby {
            let directions = self.directions_for_stop(&n.stop.stop_id, 6)?;
            let served = n.served && !directions.is_empty();
            if !served && !keep_unserved {
                continue;
            }
            out.push(StopChoice {
                stop: n.stop,
                metres: n.metres,
                served,
                directions,
            });
        }
        Ok(out)
    }

    /// Prochains passages prévus à un arrêt dans `[from_secs, from_secs + horizon]`
    /// (secondes depuis minuit du jour de service), **uniquement les services
    /// actifs** (`services`). Sans filtre, on mélangerait samedi, dimanche, vacances…
    pub fn departures_at_stop(
        &self,
        stop_id: &str,
        services: &[String],
        from_secs: i64,
        horizon_secs: i64,
        limit: usize,
    ) -> Result<Vec<ScheduledDeparture>> {
        if services.is_empty() {
            return Ok(Vec::new());
        }
        // Placeholders tous numérotés (?5, ?6, …) pour un ordre explicite.
        let placeholders = (0..services.len())
            .map(|i| format!("?{}", i + 5))
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT st.trip_id, t.route_id, st.stop_sequence, t.headsign, st.departure_secs
             FROM gtfs_stop_times st
             JOIN gtfs_trips t ON t.trip_id = st.trip_id
             WHERE st.stop_id = ?1
               AND st.departure_secs IS NOT NULL
               AND st.departure_secs BETWEEN ?2 AND ?3
               AND t.service_id IN ({placeholders})
             ORDER BY st.departure_secs
             LIMIT ?4"
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let mut binds: Vec<Box<dyn rusqlite::ToSql>> = vec![
            Box::new(stop_id.to_string()),      // ?1
            Box::new(from_secs),                // ?2
            Box::new(from_secs + horizon_secs), // ?3
            Box::new(limit as i64),             // ?4
        ];
        for s in services {
            binds.push(Box::new(s.clone())); // ?5..?N
        }
        let params_ref: Vec<&dyn rusqlite::ToSql> = binds.iter().map(|b| b.as_ref()).collect();
        let rows = stmt.query_map(params_ref.as_slice(), |r| {
            Ok(ScheduledDeparture {
                trip_id: r.get(0)?,
                route_id: r.get(1)?,
                stop_sequence: r.get(2)?,
                headsign: r.get(3)?,
                departure_secs: r.get(4)?,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }
}

/// Arrêt à proximité, avec distance et desserte.
#[derive(Debug, Clone, serde::Serialize)]
pub struct NearbyStop {
    #[serde(flatten)]
    pub stop: Stop,
    pub metres: f64,
    pub served: bool,
}

/// Un sens de passage à un arrêt : ligne + destination affichée.
/// Permet de distinguer deux quais de même nom (sens opposés de la ligne).
#[derive(Debug, Clone, serde::Serialize)]
pub struct Direction {
    pub short_name: String,
    pub headsign: String,
    pub mode: TransportMode,
}

/// Un arrêt proposé à l'usager : nom, position, distance, et **destinations**
/// des lignes qui le desservent (pour choisir le bon quai).
#[derive(Debug, Clone, serde::Serialize)]
pub struct StopChoice {
    #[serde(flatten)]
    pub stop: Stop,
    pub metres: f64,
    pub served: bool,
    pub directions: Vec<Direction>,
}

impl From<Stop> for NearbyStop {
    fn from(stop: Stop) -> Self {
        Self {
            stop,
            metres: 0.0,
            served: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ScheduledDeparture {
    pub trip_id: String,
    pub route_id: String,
    pub stop_sequence: i64,
    pub headsign: String,
    pub departure_secs: i64,
}

/// Colonne de jour (`monday`..`sunday`) correspondant à une date `YYYYMMDD`.
pub fn weekday_column(yyyymmdd: &str) -> Result<&'static str> {
    let raw = yyyymmdd.to_string();
    let date = chrono::NaiveDate::parse_from_str(yyyymmdd, "%Y%m%d")
        .map_err(|e| anyhow::anyhow!("date invalide {raw}: {e}"))?;
    use chrono::Datelike;
    Ok(match date.weekday() {
        chrono::Weekday::Mon => "monday",
        chrono::Weekday::Tue => "tuesday",
        chrono::Weekday::Wed => "wednesday",
        chrono::Weekday::Thu => "thursday",
        chrono::Weekday::Fri => "friday",
        chrono::Weekday::Sat => "saturday",
        chrono::Weekday::Sun => "sunday",
    })
}

pub fn gtfs_cache_path(data_dir: &Path) -> PathBuf {
    data_dir.join("raw").join("gtfs").join("tec-gtfs.zip")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Construit un ZIP GTFS minimal mais complet (toutes les entrées attendues
    /// par `build_sqlite`).
    fn write_minimal_zip(path: &Path) {
        let mut w = zip::ZipWriter::new(std::fs::File::create(path).unwrap());
        let put = |w: &mut zip::ZipWriter<std::fs::File>, name: &str, body: &str| {
            w.start_file(name, zip::write::SimpleFileOptions::default())
                .unwrap();
            std::io::Write::write_all(w, body.as_bytes()).unwrap();
        };
        let stop = "stop_id,stop_name,stop_lat,stop_lon\nA,Arret A,50.64,5.57\n";
        let route = "route_id,route_short_name,route_long_name,route_type\nr1,1,Ligne 1,3\n";
        let trip = "trip_id,route_id,service_id,trip_headsign\nt1,r1,s1,Direction\n";
        let st =
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence\nt1,07:00:00,07:00:00,A,1\n";
        let cal = "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\ns1,1,1,1,1,1,0,0,20260101,20261231\n";
        let cald = "service_id,date,exception_type\n";
        for (n, b) in [
            ("stops.txt", stop),
            ("routes.txt", route),
            ("trips.txt", trip),
            ("stop_times.txt", st),
            ("calendar.txt", cal),
            ("calendar_dates.txt", cald),
        ] {
            put(&mut w, n, b);
        }
        w.finish().unwrap();
    }

    #[test]
    fn base_fraiche_est_reutilisee_et_chargee_dans_un_fichier_neuf() {
        let dir = tempfile::tempdir().unwrap();
        let zip_path = dir.path().join("feed.zip");
        let db_path = dir.path().join("tec.sqlite");
        write_minimal_zip(&zip_path);

        let fp = zip_fingerprint(&zip_path).unwrap();
        let stats = load_zip_into_sqlite(&zip_path, &db_path, &fp).unwrap();
        assert_eq!(stats.trips, 1);
        assert_eq!(stats.stop_times, 1);

        // L'empreinte et la version sont bien mémorisées, et la base est
        // considérée réutilisable.
        assert!(is_reusable(&db_path, &fp).unwrap());
        let conn = Connection::open(&db_path).unwrap();
        assert_eq!(read_meta(&conn, "schema_version").as_deref(), Some("1"));
        assert_eq!(
            read_meta(&conn, "zip_fingerprint").as_deref(),
            Some(fp.as_str())
        );
        // Aucun fichier de construction ne doit subsister.
        assert!(!staging_path(&db_path).exists());
    }

    #[test]
    fn empreinte_changee_ou_base_absente_force_le_rechargement() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("tec.sqlite");
        // Base absente.
        assert!(!is_reusable(&db_path, "peu-importe").unwrap());

        let zip_path = dir.path().join("feed.zip");
        write_minimal_zip(&zip_path);
        let fp = zip_fingerprint(&zip_path).unwrap();
        load_zip_into_sqlite(&zip_path, &db_path, &fp).unwrap();
        assert!(is_reusable(&db_path, &fp).unwrap());

        // Nouveau ZIP (même taille possible, mtime différent) -> rechargement.
        assert!(!is_reusable(&db_path, "autre-empreinte").unwrap());
    }

    #[test]
    fn base_incomplete_est_rechargee() {
        let dir = tempfile::tempdir().unwrap();
        let zip_path = dir.path().join("feed.zip");
        let db_path = dir.path().join("tec.sqlite");
        write_minimal_zip(&zip_path);
        let fp = zip_fingerprint(&zip_path).unwrap();
        load_zip_into_sqlite(&zip_path, &db_path, &fp).unwrap();

        // Symptôme d'un ETL interrompu : trips chargés, stop_times vides. La
        // base paraît saine côté trips, elle doit pourtant être rejetée.
        let conn = Connection::open(&db_path).unwrap();
        conn.execute("DELETE FROM gtfs_stop_times", []).unwrap();
        drop(conn);

        assert!(!is_reusable(&db_path, &fp).unwrap());
    }

    #[test]
    fn etl_interrompu_laisse_la_base_anterieure_intacte() {
        let dir = tempfile::tempdir().unwrap();
        let zip_path = dir.path().join("feed.zip");
        let db_path = dir.path().join("tec.sqlite");
        write_minimal_zip(&zip_path);
        let fp = zip_fingerprint(&zip_path).unwrap();
        load_zip_into_sqlite(&zip_path, &db_path, &fp).unwrap();

        // Un ZIP sans `stop_times.txt` fait échouer le chargement au milieu.
        let bad_zip = dir.path().join("bad.zip");
        {
            let mut w = zip::ZipWriter::new(std::fs::File::create(&bad_zip).unwrap());
            let opt = zip::write::SimpleFileOptions::default();
            w.start_file("stops.txt", opt).unwrap();
            std::io::Write::write_all(
                &mut w,
                b"stop_id,stop_name,stop_lat,stop_lon\nB,B,50.1,5.1\n",
            )
            .unwrap();
            w.finish().unwrap();
        }
        assert!(load_zip_into_sqlite(&bad_zip, &db_path, "autre").is_err());

        // L'échec ne doit avoir ni touché la base, ni laissé de déchet.
        assert!(is_reusable(&db_path, &fp).unwrap());
        assert!(!staging_path(&db_path).exists());
    }

    #[test]
    fn parse_heure_gtfs() {
        assert_eq!(parse_gtfs_time("00:00:00"), Some(0));
        assert_eq!(parse_gtfs_time("15:32:00"), Some(55_920));
        assert_eq!(parse_gtfs_time("25:10:05"), Some(90_605)); // course de nuit
        assert_eq!(parse_gtfs_time("invalid"), None);
        assert_eq!(parse_gtfs_time(""), None);
    }

    #[test]
    fn mode_depuis_route_type() {
        assert_eq!(route_type_to_mode(0), TransportMode::Tram);
        assert_eq!(route_type_to_mode(1), TransportMode::Metro);
        assert_eq!(route_type_to_mode(2), TransportMode::Train);
        assert_eq!(route_type_to_mode(3), TransportMode::Bus);
    }

    #[test]
    fn mapping_constantes() {
        assert_eq!(calendar_columns()[0], "service_id");
        assert!(TEC_GTFS_URL.contains("tec/static"));
        assert!(
            gtfs_cache_path(Path::new("data"))
                .to_string_lossy()
                .contains("tec-gtfs.zip")
        );
    }

    #[test]
    fn erreur_si_colonne_absente() {
        let rec = csv::StringRecord::from(vec!["a", "b"]);
        assert!(header_index(&rec, "zzz").is_err());
        assert_eq!(header_index(&rec, "a").unwrap(), 0);
    }

    #[test]
    fn nearby_tri_par_distance_et_desserte() {
        let repo = GtfsRepo::open_in_memory().unwrap();
        repo.init_schema().unwrap();
        // Arrêt proche mais non desservi, plus loin mais desservi.
        repo.seed_stop(
            "near_unserved",
            "Proche non desservi",
            50.6430,
            5.5730,
            false,
        )
        .unwrap();
        repo.seed_stop("far_served", "Loin desservi", 50.6445, 5.5730, true)
            .unwrap();

        let got = repo.nearby_stops(50.6431, 5.5734, 1000.0).unwrap();
        assert_eq!(got.len(), 2);
        // Le desservi passe devant, malgré une distance plus grande.
        assert_eq!(got[0].stop.stop_id, "far_served");
        assert!(got[0].served);
        assert!(!got[1].served);
        // nearest_stop renvoie bien le premier (desservi).
        let nearest = repo.nearest_stop(50.6431, 5.5734, 1000.0).unwrap().unwrap();
        assert_eq!(nearest.stop_id, "far_served");
    }

    #[test]
    fn nearby_respecte_le_rayon() {
        let repo = GtfsRepo::open_in_memory().unwrap();
        repo.init_schema().unwrap();
        repo.seed_stop("a", "A", 50.6430, 5.5730, true).unwrap();
        repo.seed_stop("b", "B", 50.7000, 5.7000, true).unwrap(); // loin
        let got = repo.nearby_stops(50.6431, 5.5734, 500.0).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].stop.stop_id, "a");
    }
}
