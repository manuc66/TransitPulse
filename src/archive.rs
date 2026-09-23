//! Archive append-only du temps réel (SQLite).
//!
//! Les feeds GTFS-RT sont écrasés toutes les 30 s : ce qui n'est pas capturé
//! est perdu à jamais. Cette archive est donc la matière première irremplaçable
//! des futures statistiques de régularité (rue, ligne, arrêt, jour, tranche horaire).

use std::path::Path;

use anyhow::Result;
use rusqlite::{Connection, params};

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS rt_observations (
    feed_ts               INTEGER NOT NULL,
    captured_at           INTEGER NOT NULL,
    trip_id               TEXT    NOT NULL,
    route_id              TEXT,
    stop_id               TEXT    NOT NULL,
    stop_sequence         INTEGER NOT NULL,
    scheduled_ms          INTEGER,
    predicted_ms          INTEGER,
    delay_s               INTEGER,
    schedule_relationship INTEGER NOT NULL,
    PRIMARY KEY (feed_ts, trip_id, stop_sequence)
);
CREATE INDEX IF NOT EXISTS idx_obs_stop_ts  ON rt_observations(stop_id, feed_ts);
CREATE INDEX IF NOT EXISTS idx_obs_route_ts ON rt_observations(route_id, feed_ts);
CREATE INDEX IF NOT EXISTS idx_obs_trip     ON rt_observations(trip_id);

CREATE TABLE IF NOT EXISTS rt_alerts (
    feed_ts        INTEGER NOT NULL,
    captured_at    INTEGER NOT NULL,
    alert_id       TEXT    NOT NULL,
    agency_id      TEXT,
    effect         INTEGER,
    severity       INTEGER,
    cause          INTEGER,
    route_ids      TEXT,
    stop_ids       TEXT,
    header_fr      TEXT,
    description_fr TEXT,
    active_from    INTEGER,
    active_to      INTEGER,
    PRIMARY KEY (feed_ts, alert_id)
);
CREATE INDEX IF NOT EXISTS idx_alerts_route ON rt_alerts(route_ids, feed_ts);

CREATE TABLE IF NOT EXISTS rt_alert_trips (
    feed_ts     INTEGER NOT NULL,
    alert_id    TEXT    NOT NULL,
    trip_id     TEXT    NOT NULL,
    start_date  TEXT,
    PRIMARY KEY (feed_ts, alert_id, trip_id)
);
CREATE INDEX IF NOT EXISTS idx_alert_trips_trip ON rt_alert_trips(trip_id, feed_ts);
"#;

/// Une observation de passage issue du feed `trip-update`.
///
/// `scheduled_ms` est reconstruit depuis `predicted_ms - delay_s * 1000`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Observation {
    pub feed_ts: i64,
    pub captured_at: i64,
    pub trip_id: String,
    pub route_id: Option<String>,
    pub stop_id: String,
    pub stop_sequence: i64,
    pub scheduled_ms: Option<i64>,
    pub predicted_ms: Option<i64>,
    pub delay_s: Option<i64>,
    pub schedule_relationship: i64,
}

/// Une alerte de service issue du feed `alert`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlertRecord {
    pub feed_ts: i64,
    pub captured_at: i64,
    pub alert_id: String,
    pub agency_id: Option<String>,
    pub effect: Option<i64>,
    pub severity: Option<i64>,
    pub cause: Option<i64>,
    pub route_ids: String,
    pub stop_ids: String,
    pub header_fr: Option<String>,
    pub description_fr: Option<String>,
    pub active_from: Option<i64>,
    pub active_to: Option<i64>,
}

/// Lien alerte ↔ course : TEC cible les annulations sur une course précise.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlertTrip {
    pub feed_ts: i64,
    pub alert_id: String,
    pub trip_id: String,
    pub start_date: Option<String>,
}

pub struct Archive {
    conn: Connection,
}

impl Archive {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        Self::init(Connection::open(path)?)
    }

    pub fn open_in_memory() -> Result<Self> {
        Self::init(Connection::open_in_memory()?)
    }

    fn init(conn: Connection) -> Result<Self> {
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.execute_batch(SCHEMA)?;
        Ok(Self { conn })
    }

    /// Insère les observations. Idempotent : une même clé n'est écrite qu'une fois.
    pub fn insert_observations(&mut self, rows: &[Observation]) -> Result<usize> {
        let tx = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT OR REPLACE INTO rt_observations
                 (feed_ts, captured_at, trip_id, route_id, stop_id, stop_sequence,
                  scheduled_ms, predicted_ms, delay_s, schedule_relationship)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            )?;
            for r in rows {
                stmt.execute(params![
                    r.feed_ts,
                    r.captured_at,
                    r.trip_id,
                    r.route_id,
                    r.stop_id,
                    r.stop_sequence,
                    r.scheduled_ms,
                    r.predicted_ms,
                    r.delay_s,
                    r.schedule_relationship,
                ])?;
            }
        }
        tx.commit()?;
        Ok(rows.len())
    }

    pub fn insert_alerts(&mut self, rows: &[AlertRecord]) -> Result<usize> {
        let tx = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT OR REPLACE INTO rt_alerts
                 (feed_ts, captured_at, alert_id, agency_id, effect, severity, cause,
                  route_ids, stop_ids, header_fr, description_fr, active_from, active_to)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            )?;
            for r in rows {
                stmt.execute(params![
                    r.feed_ts,
                    r.captured_at,
                    r.alert_id,
                    r.agency_id,
                    r.effect,
                    r.severity,
                    r.cause,
                    r.route_ids,
                    r.stop_ids,
                    r.header_fr,
                    r.description_fr,
                    r.active_from,
                    r.active_to,
                ])?;
            }
        }
        tx.commit()?;
        Ok(rows.len())
    }

    pub fn insert_alert_trips(&mut self, rows: &[AlertTrip]) -> Result<usize> {
        let tx = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT OR REPLACE INTO rt_alert_trips (feed_ts, alert_id, trip_id, start_date)
                 VALUES (?1, ?2, ?3, ?4)",
            )?;
            for r in rows {
                stmt.execute(params![r.feed_ts, r.alert_id, r.trip_id, r.start_date])?;
            }
        }
        tx.commit()?;
        Ok(rows.len())
    }

    pub fn alert_trip_count(&self) -> Result<i64> {
        Ok(self
            .conn
            .query_row("SELECT COUNT(*) FROM rt_alert_trips", [], |r| r.get(0))?)
    }

    pub fn observation_count(&self) -> Result<i64> {
        Ok(self
            .conn
            .query_row("SELECT COUNT(*) FROM rt_observations", [], |r| r.get(0))?)
    }

    pub fn alert_count(&self) -> Result<i64> {
        Ok(self
            .conn
            .query_row("SELECT COUNT(*) FROM rt_alerts", [], |r| r.get(0))?)
    }

    /// Dernier `feed_ts` observé, toutes tables confondues.
    pub fn last_feed_ts(&self) -> Result<Option<i64>> {
        Ok(self.conn.query_row(
            "SELECT MAX(feed_ts) FROM (
                SELECT feed_ts FROM rt_observations
                UNION ALL SELECT feed_ts FROM rt_alerts)",
            [],
            |r| r.get(0),
        )?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obs(feed_ts: i64, trip: &str, seq: i64) -> Observation {
        Observation {
            feed_ts,
            captured_at: feed_ts + 5,
            trip_id: trip.into(),
            route_id: Some("gr:tec:L1".into()),
            stop_id: "gs:tec:A".into(),
            stop_sequence: seq,
            scheduled_ms: Some(1_000),
            predicted_ms: Some(1_060),
            delay_s: Some(60),
            schedule_relationship: 0,
        }
    }

    #[test]
    fn insert_et_compte() {
        let mut a = Archive::open_in_memory().unwrap();
        assert_eq!(
            a.insert_observations(&[obs(100, "t1", 1), obs(100, "t1", 2)])
                .unwrap(),
            2
        );
        assert_eq!(a.observation_count().unwrap(), 2);
        assert_eq!(a.last_feed_ts().unwrap(), Some(100));
    }

    #[test]
    fn insert_idempotent_sur_la_cle() {
        let mut a = Archive::open_in_memory().unwrap();
        a.insert_observations(&[obs(100, "t1", 1)]).unwrap();
        // Même (feed_ts, trip_id, stop_sequence) -> remplacement, pas de doublon.
        a.insert_observations(&[obs(100, "t1", 1)]).unwrap();
        assert_eq!(a.observation_count().unwrap(), 1);
        // Nouveau feed_ts -> nouvelle ligne.
        a.insert_observations(&[obs(130, "t1", 1)]).unwrap();
        assert_eq!(a.observation_count().unwrap(), 2);
    }

    #[test]
    fn liens_alerte_course_inseres() {
        let mut a = Archive::open_in_memory().unwrap();
        let rows = vec![
            AlertTrip {
                feed_ts: 200,
                alert_id: "rs:tec:1".into(),
                trip_id: "gt:tec:t1".into(),
                start_date: Some("20260924".into()),
            },
            AlertTrip {
                feed_ts: 200,
                alert_id: "rs:tec:1".into(),
                trip_id: "gt:tec:t1".into(), // doublon -> idempotent
                start_date: None,
            },
        ];
        a.insert_alert_trips(&rows).unwrap();
        assert_eq!(a.alert_trip_count().unwrap(), 1);
        a.insert_alert_trips(&[AlertTrip {
            feed_ts: 230,
            alert_id: "rs:tec:1".into(),
            trip_id: "gt:tec:t1".into(),
            start_date: None,
        }])
        .unwrap();
        assert_eq!(a.alert_trip_count().unwrap(), 2);
    }

    #[test]
    fn alertes_inserees() {
        let mut a = Archive::open_in_memory().unwrap();
        let al = AlertRecord {
            feed_ts: 200,
            captured_at: 205,
            alert_id: "rs:tec:1".into(),
            agency_id: Some("tec".into()),
            effect: Some(1),
            severity: Some(3),
            cause: Some(2),
            route_ids: "gr:tec:L1,gr:tec:L2".into(),
            stop_ids: "".into(),
            header_fr: Some("Annulations".into()),
            description_fr: Some("Ligne 1".into()),
            active_from: Some(1),
            active_to: Some(2),
        };
        assert_eq!(a.insert_alerts(&[al.clone(), al]).unwrap(), 2);
        assert_eq!(a.alert_count().unwrap(), 1);
    }
}
