//! Ingestion du GTFS statique (horaires théoriques) vers SQLite.
//!
//! Source MVP : TEC Wallonie. Le ZIP officiel (~85 Mo) est mis en cache sous
//! `data/raw/gtfs/` ; ses CSV sont lus en streaming (jamais tout en RAM) et
//! chargés dans `data/tec.sqlite` (`shapes.txt` est ignoré).
//!
//! Le schéma SQLite et la lecture associée (dépôt `GtfsRepo`) vivent ici.

use std::io::{BufRead, BufReader, Read, Seek};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use rusqlite::{Connection, params};

use crate::domain::{Line, Stop, TransportMode};

/// GTFS statique officiel TEC (Wallonie) via Belgian Mobility.
pub const TEC_GTFS_URL: &str = "https://opendata-discovery-gtfs-static.api.production.belgianmobility.io/api/gtfs/feed/tec/static";
/// Repli (portail opendata de l'opérateur).
pub const TEC_GTFS_URL_FALLBACK: &str = "https://opendata.tec-wl.be/Current%20GTFS/TEC-GTFS.zip";

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
CREATE INDEX IF NOT EXISTS idx_st_idx_stop  ON gtfs_stop_times(stop_id);
CREATE INDEX IF NOT EXISTS idx_st_idx_trip  ON gtfs_stop_times(trip_id);

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

/// Télécharge (si absent) le ZIP GTFS sous `cache_path` et le charge dans SQLite.
pub async fn fetch_and_load(
    client: &reqwest::Client,
    url: &str,
    cache_path: &Path,
    db_path: &Path,
) -> Result<GtfsStats> {
    if !cache_path.exists() {
        if let Some(parent) = cache_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let bytes = download_with_fallback(client, url, cache_path).await?;
        tracing::info!(size = bytes, "GTFS mis en cache");
    }
    // Le parsing est CPU/IO lourds : on le sort du runtime async.
    let cache = cache_path.to_path_buf();
    let db = db_path.to_path_buf();
    tokio::task::spawn_blocking(move || load_zip_into_sqlite(&cache, &db)).await?
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

/// Charge un ZIP GTFS dans une base SQLite (créée/écrasée).
pub fn load_zip_into_sqlite(zip_path: &Path, db_path: &Path) -> Result<GtfsStats> {
    let file = std::fs::File::open(zip_path)
        .with_context(|| format!("ouverture {}", zip_path.display()))?;
    let mut zip = zip::ZipArchive::new(file).context("lecture ZIP GTFS")?;

    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let conn = Connection::open(db_path)?;
    conn.pragma_update(None, "journal_mode", "OFF")?;
    conn.pragma_update(None, "synchronous", "OFF")?;
    // Rebuild : on vide toutes les tables avant rechargement.
    conn.execute_batch(SCHEMA)?;
    for t in [
        "gtfs_stop_times",
        "gtfs_trips",
        "gtfs_routes",
        "gtfs_stops",
        "gtfs_calendar_dates",
        "gtfs_calendar",
        "gtfs_meta",
    ] {
        conn.execute(&format!("DELETE FROM {t}"), [])?;
    }

    let mut stats = GtfsStats::default();
    {
        let tx = conn.unchecked_transaction()?;
        with_reader(&mut zip, "stops.txt", |r| load_stops(&tx, r, &mut stats))?;
        with_reader(&mut zip, "routes.txt", |r| load_routes(&tx, r, &mut stats))?;
        with_reader(&mut zip, "trips.txt", |r| load_trips(&tx, r, &mut stats))?;
        with_reader(&mut zip, "stop_times.txt", |r| {
            load_stop_times(&tx, r, &mut stats)
        })?;
        with_reader(&mut zip, "calendar.txt", |r| {
            load_calendar(&tx, r, &mut stats)
        })?;
        with_reader(&mut zip, "calendar_dates.txt", |r| {
            load_calendar_dates(&tx, r, &mut stats)
        })?;
        tx.commit()?;
    }
    // Index construits après l'insertion : beaucoup plus rapide.
    conn.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_st_idx_stop ON gtfs_stop_times(stop_id);
         CREATE INDEX IF NOT EXISTS idx_st_idx_trip ON gtfs_stop_times(trip_id);",
    )?;
    conn.execute_batch("ANALYZE;")?;
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
