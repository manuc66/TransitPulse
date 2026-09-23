//! Vue systémique du réseau : repérer les événements majeurs (grève, incident)
//! en mesurant, par commune, la part de courses qui ne circulent pas.
//!
//! La maille « commune » est déduite du nom d'arrêt TEC (`COMMUNE Lieu…`),
//! avec la zone tarifaire (`zone_id`) comme repli quand le nom ne permet pas
//! de conclure. Un pic d'annulations simultané sur une commune = signal.

use std::collections::HashMap;

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

/// Service de vue réseau (lecture seule sur GTFS + archive RT).
pub struct NetworkService {
    conn: Connection,
    thresholds: NetworkThresholds,
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
             JOIN main.rt_alerts a ON a.alert_id = at.alert_id AND a.feed_ts = at.feed_ts
             JOIN gtfs.gtfs_trips t ON t.trip_id = at.trip_id
             WHERE at.feed_ts = (SELECT MAX(feed_ts) FROM main.rt_alerts)
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

    /// Reconstruit les tables matérialisées `network_stats` (communes) et
    /// `network_zones` (zones TEC). **Coûteux** (scan des `stop_times`) : à
    /// appeler une fois par cycle RT, jamais par requête HTTP.
    pub fn rebuild_stats(&self) -> Result<usize> {
        type Acc = HashMap<String, std::collections::HashSet<String>>;
        let mut scheduled: Acc = HashMap::new();
        let mut sched_zones: Acc = HashMap::new();
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
                scheduled.entry(c).or_default().insert(trip.clone());
            }
            if let Some(z) = zone_from_route_id(&route) {
                sched_zones.entry(z).or_default().insert(trip);
            }
        }

        let has_alerts: i64 =
            self.conn
                .query_row("SELECT COUNT(*) FROM main.rt_alerts", [], |r| r.get(0))?;
        let mut cancelled: Acc = HashMap::new();
        let mut cancelled_zones: Acc = HashMap::new();
        if has_alerts > 0 {
            let mut stmt = self.conn.prepare(
                "SELECT DISTINCT s.stop_name, at.trip_id, t.route_id
                 FROM main.rt_alert_trips at
                 JOIN main.rt_alerts a ON a.alert_id = at.alert_id AND a.feed_ts = at.feed_ts
                 JOIN gtfs.gtfs_stop_times st ON st.trip_id = at.trip_id
                 JOIN gtfs.gtfs_stops s ON s.stop_id = st.stop_id
                 JOIN gtfs.gtfs_trips t ON t.trip_id = at.trip_id
                 WHERE at.feed_ts = (SELECT MAX(feed_ts) FROM main.rt_alerts)
                   AND a.effect IN (1, 2)",
            )?;
            let mut rows = stmt.query([])?;
            while let Some(r) = rows.next()? {
                let name: String = r.get(0)?;
                let trip: String = r.get(1)?;
                let route: String = r.get(2)?;
                if let Some(c) = commune_from_stop_name(&name) {
                    cancelled.entry(c).or_default().insert(trip.clone());
                }
                if let Some(z) = zone_from_route_id(&route) {
                    cancelled_zones.entry(z).or_default().insert(trip);
                }
            }
        }

        let tx = self.conn.unchecked_transaction()?;
        tx.execute("DELETE FROM main.network_stats", [])?;
        tx.execute("DELETE FROM main.network_zones", [])?;
        {
            let mut ins = tx.prepare(
                "INSERT OR REPLACE INTO main.network_stats
                 (commune, trips_scheduled, trips_cancelled) VALUES (?1, ?2, ?3)",
            )?;
            for (c, trips) in &scheduled {
                let ann = cancelled.get(c).map(|s| s.len() as i64).unwrap_or(0);
                ins.execute(params![c, trips.len() as i64, ann])?;
            }
        }
        {
            let mut ins = tx.prepare(
                "INSERT OR REPLACE INTO main.network_zones
                 (zone, trips_scheduled, trips_cancelled) VALUES (?1, ?2, ?3)",
            )?;
            for (z, trips) in &sched_zones {
                let ann = cancelled_zones.get(z).map(|s| s.len() as i64).unwrap_or(0);
                ins.execute(params![z, trips.len() as i64, ann])?;
            }
        }
        tx.commit()?;
        Ok(scheduled.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
