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
        let ratio = if scheduled > 0 {
            cancelled as f64 / scheduled as f64
        } else {
            0.0
        };
        let status = if scheduled == 0 {
            NetworkStatus::Normal
        } else if cancelled >= th.min_annulled && ratio >= th.critical_ratio {
            NetworkStatus::Critical
        } else if cancelled >= th.min_annulled && ratio >= th.degraded_ratio {
            NetworkStatus::Degraded
        } else {
            NetworkStatus::Normal
        };
        Self {
            commune,
            trips_scheduled: scheduled,
            trips_cancelled: cancelled,
            cancelled_ratio: ratio,
            status,
            status_label: status.label().to_string(),
        }
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

    /// Contexte réseau pour une commune (volet systémique vu d'une rue).
    pub fn commune(&self, commune: &str) -> Result<Option<CommuneStatus>> {
        Ok(self
            .communes(Some(commune))?
            .into_iter()
            .find(|c| c.commune.eq_ignore_ascii_case(commune)))
    }

    /// Reconstruit la table matérialisée `network_stats`. **Coûteux** (scan des
    /// `stop_times`) : à appeler une fois par cycle RT, jamais par requête HTTP.
    pub fn rebuild_stats(&self) -> Result<usize> {
        let mut scheduled: HashMap<String, std::collections::HashSet<String>> = HashMap::new();
        let mut stmt = self.conn.prepare(
            "SELECT s.stop_name, st.trip_id
             FROM gtfs.gtfs_stop_times st
             JOIN gtfs.gtfs_stops s ON s.stop_id = st.stop_id",
        )?;
        let mut rows = stmt.query([])?;
        while let Some(r) = rows.next()? {
            let name: String = r.get(0)?;
            let trip: String = r.get(1)?;
            if let Some(c) = commune_from_stop_name(&name) {
                scheduled.entry(c).or_default().insert(trip);
            }
        }

        let has_alerts: i64 =
            self.conn
                .query_row("SELECT COUNT(*) FROM main.rt_alerts", [], |r| r.get(0))?;
        let mut cancelled: HashMap<String, std::collections::HashSet<String>> = HashMap::new();
        if has_alerts > 0 {
            let mut stmt = self.conn.prepare(
                "SELECT DISTINCT s.stop_name, at.trip_id
                 FROM main.rt_alert_trips at
                 JOIN main.rt_alerts a ON a.alert_id = at.alert_id AND a.feed_ts = at.feed_ts
                 JOIN gtfs.gtfs_stop_times st ON st.trip_id = at.trip_id
                 JOIN gtfs.gtfs_stops s ON s.stop_id = st.stop_id
                 WHERE at.feed_ts = (SELECT MAX(feed_ts) FROM main.rt_alerts)
                   AND a.effect IN (1, 2)",
            )?;
            let mut rows = stmt.query([])?;
            while let Some(r) = rows.next()? {
                let name: String = r.get(0)?;
                let trip: String = r.get(1)?;
                if let Some(c) = commune_from_stop_name(&name) {
                    cancelled.entry(c).or_default().insert(trip);
                }
            }
        }

        let tx = self.conn.unchecked_transaction()?;
        tx.execute("DELETE FROM main.network_stats", [])?;
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
