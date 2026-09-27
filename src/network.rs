//! Vue systémique du réseau : repérer les événements majeurs (grève, incident)
//! en mesurant, par commune, la part de courses qui ne circulent pas.
//!
//! La maille « commune » est déduite du nom d'arrêt TEC (`COMMUNE Lieu…`),
//! avec la zone tarifaire (`zone_id`) comme repli quand le nom ne permet pas
//! de conclure. Un pic d'annulations simultané sur une commune = signal.

use std::collections::HashMap;
use std::sync::OnceLock;

use anyhow::Result;
use rusqlite::{Connection, params};

/// Seuils de sévérité réseau (part des courses annulées dans la commune).
#[derive(Debug, Clone, Copy)]
pub struct NetworkThresholds {
    /// À partir de cette part, le réseau local est « dégradé ».
    pub degraded_ratio: f64,
    /// À partir de cette part, il est « critique » (grève probable).
    pub critical_ratio: f64,
    /// Nombre minimal de courses annulées pour qu'un signal soit retenu
    /// (évite qu'une petite commune avec 1 course annulée passe « critique »).
    pub min_annulled: i64,
}

impl Default for NetworkThresholds {
    fn default() -> Self {
        Self {
            degraded_ratio: 0.2,
            critical_ratio: 0.5,
            min_annulled: 5,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkStatus {
    Normal,
    Degraded,
    Critical,
}

impl NetworkStatus {
    pub fn label(self) -> &'static str {
        match self {
            NetworkStatus::Normal => "normale",
            NetworkStatus::Degraded => "dégradée",
            NetworkStatus::Critical => "critique",
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct CommuneStatus {
    pub commune: String,
    /// Courses desservant la commune aujourd'hui (dénominateur).
    pub trips_scheduled: i64,
    /// Courses annulées par alerte / arrêt sauté.
    pub trips_cancelled: i64,
    /// Part de courses annulées (0..1).
    pub cancelled_ratio: f64,
    pub status: NetworkStatus,
    /// Libellé humain : « normale », « dégradée », « critique ».
    pub status_label: String,
}

impl CommuneStatus {
    /// Détermine le statut à partir des compteurs et des seuils.
    fn derive(commune: String, scheduled: i64, cancelled: i64, th: NetworkThresholds) -> Self {
        let status = derive_status(scheduled, cancelled, th);
        Self {
            commune,
            trips_scheduled: scheduled,
            trips_cancelled: cancelled,
            cancelled_ratio: if scheduled > 0 {
                cancelled as f64 / scheduled as f64
            } else {
                0.0
            },
            status,
            status_label: status.label().to_string(),
        }
    }
}

/// Zone/dépôt TEC déduit du préfixe de `route_id` :
/// `gr:tec:L0002-…` -> `L`. Les préfixes connus : B (Brabant), C (Charleroi),
/// H (Hainaut), L (Liège-Verviers), N (Namur), X (Luxembourg).
pub fn zone_from_route_id(route_id: &str) -> Option<String> {
    let rest = route_id.strip_prefix("gr:tec:")?;
    let c = rest.chars().next()?;
    if c.is_ascii_alphabetic() {
        Some(c.to_ascii_uppercase().to_string())
    } else {
        None
    }
}

/// Libellé lisible d'une zone TEC.
pub fn zone_label(zone: &str) -> &'static str {
    match zone {
        "B" => "Brabant wallon",
        "C" => "Charleroi",
        "H" => "Hainaut",
        "L" => "Liège-Verviers",
        "N" => "Namur",
        "X" => "Luxembourg",
        _ => "Autre",
    }
}

/// Extrait la commune d'un nom d'arrêt TEC (`LIEGE Gare des Guillemins` -> `LIEGE`).
/// Renvoie `None` si le nom est vide.
pub fn commune_from_stop_name(name: &str) -> Option<String> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return None;
    }
    // Le nom commence par la commune, éventuellement suffixée d'un code zone.
    let first = trimmed.split_whitespace().next().unwrap_or(trimmed);
    // Retire un éventuel préfixe numérique de zone, ex. "5810 LIEGE ...".
    if first.chars().all(|c| c.is_ascii_digit()) {
        return trimmed
            .split_whitespace()
            .nth(1)
            .map(|s| s.to_string())
            .or_else(|| Some(trimmed.to_string()));
    }
    Some(first.to_string())
}

/// Mesure d'annulation au niveau d'une ligne (service partiel vs total).
#[derive(Debug, Clone, serde::Serialize)]
pub struct LineStatus {
    pub route_id: String,
    pub trips_scheduled: i64,
    pub trips_cancelled: i64,
    pub cancelled_ratio: f64,
    pub status: NetworkStatus,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ZoneStatus {
    pub zone: String,
    pub label: String,
    pub trips_scheduled: i64,
    pub trips_cancelled: i64,
    pub cancelled_ratio: f64,
    pub status: NetworkStatus,
    pub status_label: String,
}

impl ZoneStatus {
    fn derive(zone: String, scheduled: i64, cancelled: i64, th: NetworkThresholds) -> Self {
        let status = derive_status(scheduled, cancelled, th);
        Self {
            label: zone_label(&zone).to_string(),
            cancelled_ratio: if scheduled > 0 {
                cancelled as f64 / scheduled as f64
            } else {
                0.0
            },
            zone,
            trips_scheduled: scheduled,
            trips_cancelled: cancelled,
            status,
            status_label: status.label().to_string(),
        }
    }
}

/// Statut réseau dérivé des compteurs (mutualisé communes/zones).
fn derive_status(scheduled: i64, cancelled: i64, th: NetworkThresholds) -> NetworkStatus {
    if scheduled == 0 {
        NetworkStatus::Normal
    } else {
        let ratio = cancelled as f64 / scheduled as f64;
        if cancelled >= th.min_annulled && ratio >= th.critical_ratio {
            NetworkStatus::Critical
        } else if cancelled >= th.min_annulled && ratio >= th.degraded_ratio {
            NetworkStatus::Degraded
        } else {
            NetworkStatus::Normal
        }
    }
}

/// Dénominateurs de la vue réseau : nombre de courses desservant chaque
/// commune / zone. Ne dépend que du GTFS **statique**, donc le même à chaque
/// cycle temps réel : on ne le calcule qu'une fois puis on le garde en cache
/// (les `HashSet` de trips ne servent qu'à dédupliquer pendant le scan).
#[derive(Debug, Clone, Default)]
struct StaticCounts {
    communes: HashMap<String, i64>,
    zones: HashMap<String, i64>,
}

/// Service de vue réseau (lecture seule sur GTFS + archive RT).
pub struct NetworkService {
    conn: Connection,
    thresholds: NetworkThresholds,
    /// Dénominateurs statiques, calculés au premier `rebuild_stats`.
    static_counts: OnceLock<StaticCounts>,
}

impl NetworkService {
    pub fn open(
        gtfs_path: impl AsRef<std::path::Path>,
        archive_path: impl AsRef<std::path::Path>,
    ) -> Result<Self> {
        let conn = Connection::open(archive_path)?;
        let gtfs_path = gtfs_path.as_ref();
        conn.execute(
            "ATTACH DATABASE ?1 AS gtfs",
            params![gtfs_path.to_string_lossy()],
        )?;
        Ok(Self {
            conn,
            thresholds: NetworkThresholds::default(),
            static_counts: OnceLock::new(),
        })
    }

    pub fn with_thresholds(mut self, thresholds: NetworkThresholds) -> Self {
        self.thresholds = thresholds;
        self
    }

    /// Statut réseau par commune, lu depuis la table matérialisée
    /// `network_stats` (voir `rebuild_stats`). Requête O(communes).
    /// `commune_filter` restreint à une commune si fourni.
    pub fn communes(&self, commune_filter: Option<&str>) -> Result<Vec<CommuneStatus>> {
        let has_stats: i64 =
            self.conn
                .query_row("SELECT COUNT(*) FROM main.network_stats", [], |r| r.get(0))?;
        if has_stats == 0 {
            return Ok(Vec::new());
        }
        let mut stmt = self.conn.prepare(
            "SELECT commune, trips_scheduled, trips_cancelled
             FROM main.network_stats
             WHERE (?1 IS NULL OR upper(commune) = upper(?1))",
        )?;
        let rows = stmt.query_map([commune_filter], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, i64>(2)?,
            ))
        })?;
        let mut out: Vec<CommuneStatus> = Vec::new();
        for row in rows {
            let (c, sched, ann) = row?;
            // Recalcule le statut : les seuils peuvent changer sans rebuild.
            out.push(CommuneStatus::derive(c, sched, ann, self.thresholds));
        }
        out.sort_by(|a, b| {
            b.cancelled_ratio
                .partial_cmp(&a.cancelled_ratio)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(b.trips_cancelled.cmp(&a.trips_cancelled))
        });
        Ok(out)
    }

    /// Part des courses d'une ligne annulée par alerte (service partiel vs total).
    /// Calcul ciblé (une ligne) : pas de scan global.
    pub fn line_ratio(&self, route_id: &str) -> Result<Option<LineStatus>> {
        let scheduled: i64 = self.conn.query_row(
            "SELECT COUNT(DISTINCT st.trip_id)
             FROM gtfs.gtfs_stop_times st
             JOIN gtfs.gtfs_trips t ON t.trip_id = st.trip_id
             WHERE t.route_id = ?1",
            params![route_id],
            |r| r.get(0),
        )?;
        if scheduled == 0 {
            return Ok(None);
        }
        let cancelled: i64 = self.conn.query_row(
            "SELECT COUNT(DISTINCT at.trip_id)
             FROM main.rt_alert_trips at
             JOIN main.rt_alerts a ON a.alert_id = at.alert_id
             JOIN gtfs.gtfs_trips t ON t.trip_id = at.trip_id
             WHERE a.last_seen = (SELECT MAX(last_seen) FROM main.rt_alerts)
               AND at.last_seen = a.last_seen
               AND a.effect IN (1, 2)
               AND t.route_id = ?1",
            params![route_id],
            |r| r.get(0),
        )?;
        let ratio = cancelled as f64 / scheduled as f64;
        Ok(Some(LineStatus {
            route_id: route_id.to_string(),
            trips_scheduled: scheduled,
            trips_cancelled: cancelled,
            cancelled_ratio: ratio,
            status: derive_status(scheduled, cancelled, self.thresholds),
        }))
    }

    /// Statut réseau par zone/dépôt TEC, trié par part d'annulation décroissante.
    pub fn zones(&self) -> Result<Vec<ZoneStatus>> {
        let has_stats: i64 =
            self.conn
                .query_row("SELECT COUNT(*) FROM main.network_zones", [], |r| r.get(0))?;
        if has_stats == 0 {
            return Ok(Vec::new());
        }
        let mut stmt = self
            .conn
            .prepare("SELECT zone, trips_scheduled, trips_cancelled FROM main.network_zones")?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, i64>(2)?,
            ))
        })?;
        let mut out: Vec<ZoneStatus> = Vec::new();
        for row in rows {
            let (z, sched, ann) = row?;
            out.push(ZoneStatus::derive(z, sched, ann, self.thresholds));
        }
        out.sort_by(|a, b| {
            b.cancelled_ratio
                .partial_cmp(&a.cancelled_ratio)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(b.trips_cancelled.cmp(&a.trips_cancelled))
        });
        Ok(out)
    }

    /// Contexte réseau pour une commune (volet systémique vu d'une rue).
    pub fn commune(&self, commune: &str) -> Result<Option<CommuneStatus>> {
        Ok(self
            .communes(Some(commune))?
            .into_iter()
            .find(|c| c.commune.eq_ignore_ascii_case(commune)))
    }

    /// Dénominateurs statiques : un scan des `stop_times` (5,6 M de lignes sur
    /// le réseau TEC), **coûteux** (~5 s) mais **constant** d'un cycle à l'autre
    /// puisque seul le GTFS statique entre dans le calcul. Le résultat est donc
    /// mis en cache : les appels suivants ne refont que la partie alertes.
    fn static_counts(&self) -> Result<&StaticCounts> {
        if let Some(c) = self.static_counts.get() {
            return Ok(c);
        }
        type Acc = HashMap<String, std::collections::HashSet<String>>;
        let mut by_commune: Acc = HashMap::new();
        let mut by_zone: Acc = HashMap::new();
        let mut stmt = self.conn.prepare(
            "SELECT s.stop_name, st.trip_id, t.route_id
             FROM gtfs.gtfs_stop_times st
             JOIN gtfs.gtfs_stops s ON s.stop_id = st.stop_id
             JOIN gtfs.gtfs_trips t ON t.trip_id = st.trip_id",
        )?;
        let mut rows = stmt.query([])?;
        while let Some(r) = rows.next()? {
            let name: String = r.get(0)?;
            let trip: String = r.get(1)?;
            let route: String = r.get(2)?;
            if let Some(c) = commune_from_stop_name(&name) {
                by_commune.entry(c).or_default().insert(trip.clone());
            }
            if let Some(z) = zone_from_route_id(&route) {
                by_zone.entry(z).or_default().insert(trip);
            }
        }
        // Seuls les compteurs sont réutilisés ensuite : on libère les sets.
        let counts = StaticCounts {
            communes: by_commune
                .into_iter()
                .map(|(k, v)| (k, v.len() as i64))
                .collect(),
            zones: by_zone.into_iter().map(|(k, v)| (k, v.len() as i64)).collect(),
        };
        // `set` ne peut échouer que si un autre appel a gagné la course ; le
        // service étant derrière un `Mutex`, il n'y en a pas en pratique.
        Ok(self.static_counts.get_or_init(|| counts))
    }

    /// Reconstruit les tables matérialisées `network_stats` (communes) et
    /// `network_zones` (zones TEC). Le scan du GTFS statique n'est fait qu'au
    /// premier appel (mis en cache par `static_counts`) ; les suivants ne
    /// recalculent que les annulations, en quelques millisecondes. À appeler
    /// une fois par cycle RT, jamais par requête HTTP : l'appelant doit
    /// éviter de tenir le `Mutex` pendant un premier calcul.
    pub fn rebuild_stats(&self) -> Result<usize> {
        let scheduled = self.static_counts()?;

        // `COUNT(*)` ne convient plus : la table contient toutes les alertes
        // connues, pas celles du dernier cycle. On interroge le `last_seen` max.
        let has_alerts: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM main.rt_alerts WHERE last_seen = (SELECT MAX(last_seen) FROM main.rt_alerts)",
            [],
            |r| r.get(0),
        )?;
        let mut cancelled: HashMap<String, i64> = HashMap::new();
        let mut cancelled_zones: HashMap<String, i64> = HashMap::new();
        if has_alerts > 0 {
            let mut stmt = self.conn.prepare(
                "SELECT DISTINCT s.stop_name, at.trip_id, t.route_id
                 FROM main.rt_alert_trips at
                 JOIN main.rt_alerts a ON a.alert_id = at.alert_id
                 JOIN gtfs.gtfs_stop_times st ON st.trip_id = at.trip_id
                 JOIN gtfs.gtfs_stops s ON s.stop_id = st.stop_id
                 JOIN gtfs.gtfs_trips t ON t.trip_id = at.trip_id
                 WHERE a.last_seen = (SELECT MAX(last_seen) FROM main.rt_alerts)
                   AND at.last_seen = a.last_seen
                   AND a.effect IN (1, 2)",
            )?;
            let mut rows = stmt.query([])?;
            // Un même trip traverse plusieurs arrêt d'une commune : on
            // dédoublonne par commune/zone avant de compter.
            let mut seen_c: HashMap<String, std::collections::HashSet<String>> = HashMap::new();
            let mut seen_z: HashMap<String, std::collections::HashSet<String>> = HashMap::new();
            while let Some(r) = rows.next()? {
                let name: String = r.get(0)?;
                let trip: String = r.get(1)?;
                let route: String = r.get(2)?;
                if let Some(c) = commune_from_stop_name(&name) {
                    seen_c.entry(c).or_default().insert(trip.clone());
                }
                if let Some(z) = zone_from_route_id(&route) {
                    seen_z.entry(z).or_default().insert(trip);
                }
            }
            cancelled = seen_c.into_iter().map(|(k, v)| (k, v.len() as i64)).collect();
            cancelled_zones = seen_z.into_iter().map(|(k, v)| (k, v.len() as i64)).collect();
        }

        let tx = self.conn.unchecked_transaction()?;
        tx.execute("DELETE FROM main.network_stats", [])?;
        tx.execute("DELETE FROM main.network_zones", [])?;
        {
            let mut ins = tx.prepare(
                "INSERT OR REPLACE INTO main.network_stats
                 (commune, trips_scheduled, trips_cancelled) VALUES (?1, ?2, ?3)",
            )?;
            for (c, n) in &scheduled.communes {
                let ann = cancelled.get(c).copied().unwrap_or(0);
                ins.execute(params![c, n, ann])?;
            }
        }
        {
            let mut ins = tx.prepare(
                "INSERT OR REPLACE INTO main.network_zones
                 (zone, trips_scheduled, trips_cancelled) VALUES (?1, ?2, ?3)",
            )?;
            for (z, n) in &scheduled.zones {
                let ann = cancelled_zones.get(z).copied().unwrap_or(0);
                ins.execute(params![z, n, ann])?;
            }
        }
        tx.commit()?;
        Ok(scheduled.communes.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::{AlertRecord, AlertTrip, Archive};
    use crate::gtfs::GtfsRepo;

    /// Deux communes, deux zones, trois courses :
    /// LIEGE (arrêt A) desservie par t1 et t2 en zone L, NAMUR (arrêt B)
    /// par t3 en zone N.
    fn fixture(dir: &std::path::Path) -> (std::path::PathBuf, std::path::PathBuf) {
        let gtfs_path = dir.join("gtfs.sqlite");
        let archive_path = dir.join("archive.sqlite");
        let g = GtfsRepo::create_schema_at(&gtfs_path).unwrap();
        g.execute_batch(
            "INSERT INTO gtfs_stops (stop_id, stop_name, lat, lon) VALUES
                ('A', 'LIEGE Test', 50.64, 5.57),
                ('B', 'NAMUR Test', 50.46, 4.86);
             INSERT INTO gtfs_routes (route_id, short_name, long_name, route_type) VALUES
                ('gr:tec:L0001', '1', 'Liege', 3),
                ('gr:tec:N0002', '2', 'Namur', 3);
             INSERT INTO gtfs_trips (trip_id, route_id, service_id) VALUES
                ('t1', 'gr:tec:L0001', 'svc'),
                ('t2', 'gr:tec:L0001', 'svc'),
                ('t3', 'gr:tec:N0002', 'svc');
             INSERT INTO gtfs_stop_times (trip_id, stop_sequence, stop_id, departure_secs) VALUES
                ('t1', 1, 'A', 36000),
                ('t2', 1, 'A', 37800),
                ('t3', 1, 'B', 36000);",
        )
        .unwrap();
        drop(g);
        Archive::open(&archive_path).unwrap();
        (gtfs_path, archive_path)
    }

    /// Alerte NO_SERVICE ciblant une course, à un `feed_ts` donné.
    fn alerte(archive_path: &std::path::Path, feed_ts: i64, alert_id: &str, trip_id: &str) {
        let mut a = Archive::open(archive_path).unwrap();
        a.insert_alerts(&[AlertRecord {
            feed_ts,
            captured_at: feed_ts,
            alert_id: alert_id.into(),
            agency_id: Some("tec".into()),
            effect: Some(1), // NO_SERVICE
            severity: Some(2),
            cause: Some(2),
            route_ids: "gr:tec:L0001".into(),
            stop_ids: String::new(),
            header_fr: Some("Annulation".into()),
            description_fr: None,
            active_from: None,
            active_to: None,
        }])
        .unwrap();
        a.insert_alert_trips(&[AlertTrip {
            feed_ts,
            alert_id: alert_id.into(),
            trip_id: trip_id.into(),
            start_date: None,
        }])
        .unwrap();
    }

    #[test]
    fn rebuild_compte_les_courses_par_commune_et_par_zone() {
        let dir = tempfile::tempdir().unwrap();
        let (gtfs_path, archive_path) = fixture(dir.path());
        let feed_ts = 1_800_000_000;
        alerte(&archive_path, feed_ts, "rs:tec:1", "t1");

        let svc = NetworkService::open(&gtfs_path, &archive_path).unwrap();
        assert_eq!(svc.rebuild_stats().unwrap(), 2, "deux communes");

        let communes = svc.communes(None).unwrap();
        assert_eq!(communes.len(), 2);
        let liege = communes.iter().find(|c| c.commune == "LIEGE").unwrap();
        assert_eq!(liege.trips_scheduled, 2, "t1 et t2");
        assert_eq!(liege.trips_cancelled, 1, "seule t1 est annulée");
        let namur = communes.iter().find(|c| c.commune == "NAMUR").unwrap();
        assert_eq!(namur.trips_scheduled, 1);
        assert_eq!(namur.trips_cancelled, 0);

        let zones = svc.zones().unwrap();
        assert_eq!(zones.len(), 2);
        let l = zones.iter().find(|z| z.zone == "L").unwrap();
        assert_eq!((l.trips_scheduled, l.trips_cancelled), (2, 1));
        let n = zones.iter().find(|z| z.zone == "N").unwrap();
        assert_eq!((n.trips_scheduled, n.trips_cancelled), (1, 0));
    }

    /// Le scan du GTFS statique est mis en cache : le reconstruire ne doit pas
    /// changer le résultat, mais les annulations, elles, doivent être relues à
    /// chaque appel (c'est le seul volet dynamique de la vue).
    ///
    /// Comme dans `rebuild_stats`, seul le `feed_ts` le plus récent des alertes
    /// compte : chaque cycle RT renvoie la totalité des alertes courantes, on
    /// ne fait donc pas d'union avec les feeds précédents.
    #[test]
    fn rebuild_avec_cache_reste_frais_apres_une_nouvelle_alerte() {
        let dir = tempfile::tempdir().unwrap();
        let (gtfs_path, archive_path) = fixture(dir.path());
        let t0 = 1_800_000_000;
        alerte(&archive_path, t0, "rs:tec:1", "t1");

        let svc = NetworkService::open(&gtfs_path, &archive_path).unwrap();
        svc.rebuild_stats().unwrap();

        // Deuxième appel à l'identique : le cache statique ne doit rien changer.
        svc.rebuild_stats().unwrap();
        let liege = svc.commune("LIEGE").unwrap().unwrap();
        assert_eq!(
            (liege.trips_scheduled, liege.trips_cancelled),
            (2, 1),
            "rebuild idempotent"
        );

        // Feed plus récent annulant t1 *et* t2 : le dénominateur statique est
        // réutilisé tel quel, le numérateur est recalculé depuis les alertes.
        alerte(&archive_path, t0 + 30, "rs:tec:2", "t1");
        alerte(&archive_path, t0 + 30, "rs:tec:3", "t2");
        svc.rebuild_stats().unwrap();
        let liege = svc.commune("LIEGE").unwrap().unwrap();
        assert_eq!(
            (liege.trips_scheduled, liege.trips_cancelled),
            (2, 2),
            "la nouvelle alerte doit être prise en compte malgré le cache"
        );
        // La zone suit le même décompte que la commune.
        let l = svc.zones().unwrap().into_iter().find(|z| z.zone == "L").unwrap();
        assert_eq!((l.trips_scheduled, l.trips_cancelled), (2, 2));
    }

    #[test]
    fn commune_depuis_nom() {
        assert_eq!(
            commune_from_stop_name("LIEGE Gare des Guillemins"),
            Some("LIEGE".into())
        );
        assert_eq!(
            commune_from_stop_name("  SCLESSIN Standard  "),
            Some("SCLESSIN".into())
        );
        assert_eq!(commune_from_stop_name(""), None);
        assert_eq!(
            commune_from_stop_name("5810 LIEGE Centre"),
            Some("LIEGE".into())
        );
    }

    #[test]
    fn zone_depuis_route_id() {
        assert_eq!(
            zone_from_route_id("gr:tec:L0002-24115").as_deref(),
            Some("L")
        );
        assert_eq!(
            zone_from_route_id("gr:tec:H4008-22717").as_deref(),
            Some("H")
        );
        assert_eq!(
            zone_from_route_id("gr:tec:C0001-21650").as_deref(),
            Some("C")
        );
        assert_eq!(zone_from_route_id("gr:tec:x123").as_deref(), Some("X"));
        assert_eq!(zone_from_route_id("rs:tec:1"), None);
        assert_eq!(zone_from_route_id("gr:tec:0001"), None);
        assert_eq!(zone_label("L"), "Liège-Verviers");
        assert_eq!(zone_label("Z"), "Autre");
    }

    #[test]
    fn statut_derive_selon_seuils() {
        let th = NetworkThresholds::default();
        let normal = CommuneStatus::derive("A".into(), 100, 3, th);
        assert_eq!(normal.status, NetworkStatus::Normal); // sous min_annulled
        let degraded = CommuneStatus::derive("B".into(), 100, 25, th);
        assert_eq!(degraded.status, NetworkStatus::Degraded);
        assert!((degraded.cancelled_ratio - 0.25).abs() < 1e-9);
        let critical = CommuneStatus::derive("C".into(), 100, 60, th);
        assert_eq!(critical.status, NetworkStatus::Critical);
        // Petite commune : peu d'annulations -> jamais critique.
        let tiny = CommuneStatus::derive("D".into(), 3, 3, th);
        assert_eq!(tiny.status, NetworkStatus::Normal);
        // Aucun service prévu -> normal (pas de signal).
        let empty = CommuneStatus::derive("E".into(), 0, 0, th);
        assert_eq!(empty.status, NetworkStatus::Normal);
    }
}
