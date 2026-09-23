//! Ingestion temps réel officielle TEC (GTFS-RT en JSON).
//!
//! Source : API publique de Belgian Mobility (découverte), sans clé.
//! On archive chaque cycle dans SQLite (voir `archive`) : c'est la seule donnée
//! non reconstructible a posteriori.

use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use anyhow::{Context, Result};
use flate2::Compression;
use flate2::write::GzEncoder;
use serde::{Deserialize, Serialize};

use tokio::sync::broadcast;

use crate::archive::{AlertRecord, Archive, Observation};

/// GTFS-RT `trip-update` officiel TEC (JSON).
pub const DEFAULT_TRIP_UPDATES_URL: &str =
    "https://api-management-discovery-production.azure-api.net/api/gtfs/feed/tec/rt/trip-update";
/// GTFS-RT `alert` officiel TEC (JSON).
pub const DEFAULT_ALERTS_URL: &str =
    "https://api-management-discovery-production.azure-api.net/api/gtfs/feed/tec/rt/alert";
/// GTFS-RT `vehicle-position` officiel TEC (JSON).
pub const DEFAULT_VEHICLE_POSITIONS_URL: &str = "https://api-management-discovery-production.azure-api.net/api/gtfs/feed/tec/rt/vehicle-position";

/// Rétention par défaut des payloads bruts (jours). 0 = conservation illimitée.
pub const DEFAULT_RAW_RETENTION_DAYS: u64 = 90;

// --- Archive brute (filet de sécurité) --------------------------------------

/// Conserve les payloads bruts gzippés, un fichier JSONL par jour et par flux,
/// sous `root` (ex. `data/raw/rt`). Permet de rejouer/re-parser sans re-fetch.
#[derive(Debug, Clone)]
pub struct RawArchive {
    root: PathBuf,
    retention_days: u64,
}

impl RawArchive {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            retention_days: DEFAULT_RAW_RETENTION_DAYS,
        }
    }

    pub fn with_retention(mut self, days: u64) -> Self {
        self.retention_days = days;
        self
    }

    /// Ajoute une ligne JSONL gzippée pour `kind` (`trip-update`, `alert`, …).
    pub fn append(&self, kind: &str, feed_ts: i64, line_body: &str) -> Result<()> {
        let day = day_string(feed_ts);
        let dir = self.root.join(&day);
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("création du dossier brut {}", dir.display()))?;
        let path = dir.join(format!("{kind}.jsonl.gz"));
        // `GzEncoder` accepte des fichiers concaténés : append multiple = valide.
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        let mut enc = GzEncoder::new(file, Compression::fast());
        enc.write_all(line_body.as_bytes())?;
        enc.write_all(b"\n")?;
        enc.finish()?;
        Ok(())
    }

    /// Supprime les dossiers journaliers plus vieux que la rétention.
    pub fn prune(&self, now: i64) -> Result<usize> {
        if self.retention_days == 0 || !self.root.exists() {
            return Ok(0);
        }
        let cutoff = day_string(now - self.retention_days as i64 * 86_400);
        let mut removed = 0;
        for entry in std::fs::read_dir(&self.root)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();
            if name.len() == 10 && name < cutoff {
                std::fs::remove_dir_all(entry.path())?;
                removed += 1;
            }
        }
        Ok(removed)
    }
}

fn day_string(unix_secs: i64) -> String {
    use chrono::DateTime;
    DateTime::from_timestamp(unix_secs, 0)
        .map(|d| d.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| "unknown".to_string())
}
// --- Codes GTFS-RT (enum protobuf) ------------------------------------------

pub const SCHEDULED: i64 = 0;
pub const SKIPPED: i64 = 1;
pub const NO_DATA: i64 = 2;
pub const UNSCHEDULED: i64 = 3;

pub const EFFECT_NO_SERVICE: i64 = 1;
pub const EFFECT_REDUCED_SERVICE: i64 = 2;
pub const EFFECT_SIGNIFICANT_DELAYS: i64 = 3;
pub const EFFECT_DETOUR: i64 = 4;
pub const EFFECT_MODIFIED_SERVICE: i64 = 6;

// --- Types JSON -------------------------------------------------------------

#[derive(Debug, Deserialize, Serialize)]
pub struct Feed<T> {
    pub header: Header,
    pub entity: Vec<T>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Header {
    pub timestamp: i64,
    #[serde(rename = "gtfsRealtimeVersion", default)]
    pub version: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TripUpdateEntity {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(rename = "tripUpdate", default)]
    pub trip_update: Option<TripUpdate>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TripUpdate {
    #[serde(default)]
    pub trip: Option<TripDescriptor>,
    #[serde(rename = "stopTimeUpdate", default)]
    pub stop_time_update: Vec<StopTimeUpdate>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TripDescriptor {
    #[serde(rename = "tripId", default)]
    pub trip_id: Option<String>,
    #[serde(rename = "routeId", default)]
    pub route_id: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StopTimeUpdate {
    #[serde(rename = "stopSequence", default)]
    pub stop_sequence: Option<i64>,
    #[serde(rename = "stopId", default)]
    pub stop_id: Option<String>,
    #[serde(default)]
    pub arrival: Option<StopTimeEvent>,
    #[serde(default)]
    pub departure: Option<StopTimeEvent>,
    #[serde(rename = "scheduleRelationship", default)]
    pub schedule_relationship: Option<i64>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct StopTimeEvent {
    #[serde(default)]
    pub delay: Option<i64>,
    #[serde(default)]
    pub time: Option<i64>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct AlertEntity {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub alert: Option<Alert>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Alert {
    #[serde(rename = "activePeriod", default)]
    pub active_period: Vec<TimeRange>,
    #[serde(rename = "informedEntity", default)]
    pub informed_entity: Vec<InformedEntity>,
    #[serde(default)]
    pub cause: Option<i64>,
    #[serde(default)]
    pub effect: Option<i64>,
    #[serde(rename = "severityLevel", default)]
    pub severity: Option<i64>,
    #[serde(rename = "headerText", default)]
    pub header_text: Option<TranslatedString>,
    #[serde(rename = "descriptionText", default)]
    pub description_text: Option<TranslatedString>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TimeRange {
    #[serde(default)]
    pub start: Option<i64>,
    #[serde(default)]
    pub end: Option<i64>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct InformedEntity {
    #[serde(rename = "agencyId", default)]
    pub agency_id: Option<String>,
    #[serde(rename = "routeId", default)]
    pub route_id: Option<String>,
    #[serde(rename = "stopId", default)]
    pub stop_id: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TranslatedString {
    #[serde(default)]
    pub translation: Vec<Translation>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Translation {
    #[serde(default)]
    pub language: Option<String>,
    #[serde(default)]
    pub text: Option<String>,
}

impl TranslatedString {
    fn fr(&self) -> Option<String> {
        self.translation
            .iter()
            .find(|t| t.language.as_deref() == Some("fr"))
            .or_else(|| self.translation.first())
            .and_then(|t| t.text.clone())
    }
}

// --- Conversion -------------------------------------------------------------

/// Construit les observations archivables à partir d'un feed `trip-update`.
///
/// On ne conserve que les arrêts utiles : prédiction (arrivée/départ) ou
/// annulation (`SKIPPED`). Les `NO_DATA` (arrêts passés/sans prédiction) sont ignorés.
pub fn observations_from(
    feed_ts: i64,
    captured_at: i64,
    entities: &[TripUpdateEntity],
) -> Vec<Observation> {
    let mut out = Vec::new();
    for e in entities {
        let Some(tu) = &e.trip_update else { continue };
        let (trip_id, route_id) = match &tu.trip {
            Some(t) => (t.trip_id.clone(), t.route_id.clone()),
            None => (None, None),
        };
        let Some(trip_id) = trip_id else { continue };

        for stu in &tu.stop_time_update {
            let Some(stop_id) = stu.stop_id.clone() else {
                continue;
            };
            let Some(stop_sequence) = stu.stop_sequence else {
                continue;
            };
            let relationship = stu.schedule_relationship.unwrap_or(SCHEDULED);
            let event = stu.departure.or(stu.arrival);

            let is_cancelled = relationship == SKIPPED;
            if !is_cancelled && (event.is_none() || event.and_then(|e| e.time).is_none()) {
                continue;
            }

            let predicted_ms = event.and_then(|e| e.time).map(|t| t * 1000);
            let delay_s = event.and_then(|e| e.delay);
            let scheduled_ms = match (predicted_ms, delay_s) {
                (Some(p), Some(d)) => Some(p - d * 1000),
                _ => None,
            };

            out.push(Observation {
                feed_ts,
                captured_at,
                trip_id: trip_id.clone(),
                route_id: route_id.clone(),
                stop_id,
                stop_sequence,
                scheduled_ms,
                predicted_ms,
                delay_s,
                schedule_relationship: relationship,
            });
        }
    }
    out
}

/// Construit les alertes archivables à partir d'un feed `alert`.
pub fn alerts_from(feed_ts: i64, captured_at: i64, entities: &[AlertEntity]) -> Vec<AlertRecord> {
    let mut out = Vec::new();
    for e in entities {
        let Some(alert) = &e.alert else { continue };
        let Some(alert_id) = e.id.clone() else {
            continue;
        };

        let agency_id = alert
            .informed_entity
            .iter()
            .find_map(|i| i.agency_id.clone());
        let route_ids: Vec<String> = alert
            .informed_entity
            .iter()
            .filter_map(|i| i.route_id.clone())
            .collect();
        let stop_ids: Vec<String> = alert
            .informed_entity
            .iter()
            .filter_map(|i| i.stop_id.clone())
            .collect();
        let period = alert.active_period.first();

        out.push(AlertRecord {
            feed_ts,
            captured_at,
            alert_id,
            agency_id,
            effect: alert.effect,
            severity: alert.severity,
            cause: alert.cause,
            route_ids: route_ids.join(","),
            stop_ids: stop_ids.join(","),
            header_fr: alert.header_text.as_ref().and_then(|t| t.fr()),
            description_fr: alert.description_text.as_ref().and_then(|t| t.fr()),
            active_from: period.and_then(|p| p.start),
            active_to: period.and_then(|p| p.end),
        });
    }
    out
}

// --- Récupération réseau ----------------------------------------------------

pub async fn fetch_trip_updates(
    client: &reqwest::Client,
    url: &str,
) -> Result<Feed<TripUpdateEntity>> {
    Ok(client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}

pub async fn fetch_alerts(client: &reqwest::Client, url: &str) -> Result<Feed<AlertEntity>> {
    Ok(client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}

pub async fn fetch_vehicle_positions(
    client: &reqwest::Client,
    url: &str,
) -> Result<Feed<serde_json::Value>> {
    Ok(client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}

// --- État partagé / configuration -------------------------------------------

#[derive(Debug, Default)]
pub struct RtState {
    /// Dernier cycle réussi (unix secs, horloge locale).
    pub last_success: Option<i64>,
    /// Timestamp du feed officiel du dernier cycle.
    pub last_feed_ts: Option<i64>,
    pub observations_last: usize,
    pub alerts_last: usize,
    pub last_error: Option<String>,
}

/// Événement diffusé à chaque cycle RT réussi : les abonnés (WebSocket) savent
/// alors qu'il y a du nouveau et peuvent recalculer leur statut.
#[derive(Debug, Clone, Copy)]
pub struct LiveEvent {
    pub feed_ts: i64,
    pub observations: usize,
    pub alerts: usize,
}

impl RtState {
    pub fn feed_age_secs(&self, now: i64) -> Option<i64> {
        self.last_success.map(|t| now - t)
    }

    pub fn is_stale(&self, now: i64, stale_after: i64) -> bool {
        match self.feed_age_secs(now) {
            Some(age) => age > stale_after,
            None => true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RtConfig {
    pub trip_updates_url: String,
    pub alerts_url: String,
    pub poll_interval: Duration,
}

impl Default for RtConfig {
    fn default() -> Self {
        Self {
            trip_updates_url: DEFAULT_TRIP_UPDATES_URL.to_string(),
            alerts_url: DEFAULT_ALERTS_URL.to_string(),
            poll_interval: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PollOutcome {
    pub feed_ts: i64,
    pub observations: usize,
    pub alerts: usize,
}

/// Un cycle complet : fetch des feeds, archivage brut, conversion, persistance.
pub async fn poll_once(
    client: &reqwest::Client,
    archive: &Arc<Mutex<Archive>>,
    raw: &RawArchive,
    cfg: &RtConfig,
) -> Result<PollOutcome> {
    let captured_at = chrono::Utc::now().timestamp();

    let tu = fetch_trip_updates(client, &cfg.trip_updates_url).await?;
    let al = fetch_alerts(client, &cfg.alerts_url).await?;

    let obs = observations_from(tu.header.timestamp, captured_at, &tu.entity);
    let alerts = alerts_from(al.header.timestamp, captured_at, &al.entity);
    let counts = PollOutcome {
        feed_ts: tu.header.timestamp,
        observations: obs.len(),
        alerts: alerts.len(),
    };

    let raw = raw.clone();
    let tu_line = serde_json::to_string(&tu)?;
    let al_line = serde_json::to_string(&al)?;
    let archive = archive.clone();
    tokio::task::spawn_blocking(move || -> Result<()> {
        raw.append("trip-update", counts.feed_ts, &tu_line)?;
        raw.append("alert", counts.feed_ts, &al_line)?;
        let mut a = archive.lock().expect("archive mutex empoisonné");
        a.insert_observations(&obs)?;
        a.insert_alerts(&alerts)?;
        Ok(())
    })
    .await??;

    Ok(counts)
}

/// Boucle de polling : capture le temps réel en continu et met à jour l'état.
pub async fn run(
    client: reqwest::Client,
    archive: Arc<Mutex<Archive>>,
    raw: RawArchive,
    state: Arc<RwLock<RtState>>,
    events: broadcast::Sender<LiveEvent>,
    cfg: RtConfig,
) {
    let mut cycles: u64 = 0;
    loop {
        match poll_once(&client, &archive, &raw, &cfg).await {
            Ok(o) => {
                tracing::info!(
                    feed_ts = o.feed_ts,
                    observations = o.observations,
                    alerts = o.alerts,
                    "cycle RT archivé"
                );
                {
                    let mut s = state.write().expect("état RT empoisonné");
                    s.last_success = Some(chrono::Utc::now().timestamp());
                    s.last_feed_ts = Some(o.feed_ts);
                    s.observations_last = o.observations;
                    s.alerts_last = o.alerts;
                    s.last_error = None;
                }
                // Diffuse l'événement (ignoré s'il n'y a aucun abonné).
                let _ = events.send(LiveEvent {
                    feed_ts: o.feed_ts,
                    observations: o.observations,
                    alerts: o.alerts,
                });
            }
            Err(e) => {
                tracing::warn!(error = %e, "échec du cycle RT");
                let mut s = state.write().expect("état RT empoisonné");
                s.last_error = Some(e.to_string());
            }
        }

        // Purge de rétention une fois par heure.
        cycles += 1;
        if cycles.is_multiple_of(120) {
            let raw = raw.clone();
            let now = chrono::Utc::now().timestamp();
            let _ = tokio::task::spawn_blocking(move || raw.prune(now)).await;
        }

        tokio::time::sleep(cfg.poll_interval).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TRIP_UPDATE_FIXTURE: &str = r#"{
      "header": {"gtfsRealtimeVersion": "1.0", "timestamp": 1790190210},
      "entity": [
        {"id": "rt:tec:1", "tripUpdate": {
          "trip": {"tripId": "gt:tec:1", "routeId": "gr:tec:L1"},
          "stopTimeUpdate": [
            {"stopSequence": 10, "stopId": "gs:tec:A",
             "departure": {"time": 1790184359, "delay": -60}, "scheduleRelationship": 0},
            {"stopSequence": 11, "stopId": "gs:tec:B", "scheduleRelationship": 1},
            {"stopSequence": 9, "stopId": "gs:tec:C", "scheduleRelationship": 2}
          ]
        }}
      ]
    }"#;

    const ALERT_FIXTURE: &str = r#"{
      "header": {"gtfsRealtimeVersion": "1.0", "timestamp": 1790190206},
      "entity": [
        {"id": "rs:tec:1", "alert": {
          "activePeriod": [{"start": 1788732060, "end": 7258114800}],
          "informedEntity": [{"agencyId": "tec", "routeId": "gr:tec:H1078-23109", "stopId": "gs:tec:X"}],
          "cause": 10, "effect": 4, "severityLevel": 1,
          "headerText": {"translation": [{"language": "fr", "text": "Travaux"}]},
          "descriptionText": {"translation": [{"language": "fr", "text": "Ligne déviée"}]}
        }}
      ]
    }"#;

    #[test]
    fn parse_trip_update_et_filtre() {
        let feed: Feed<TripUpdateEntity> = serde_json::from_str(TRIP_UPDATE_FIXTURE).unwrap();
        let obs = observations_from(feed.header.timestamp, 42, &feed.entity);
        // C est NO_DATA sans heure -> écarté. A (prédiction) et B (annulé) conservés.
        assert_eq!(obs.len(), 2);

        let a = obs.iter().find(|o| o.stop_id == "gs:tec:A").unwrap();
        assert_eq!(a.trip_id, "gt:tec:1");
        assert_eq!(a.route_id.as_deref(), Some("gr:tec:L1"));
        assert_eq!(a.predicted_ms, Some(1_790_184_359_000));
        assert_eq!(a.delay_s, Some(-60));
        // scheduled = predicted - delay*1000
        assert_eq!(a.scheduled_ms, Some(1_790_184_419_000));
        assert_eq!(a.captured_at, 42);

        let b = obs.iter().find(|o| o.stop_id == "gs:tec:B").unwrap();
        assert_eq!(b.schedule_relationship, SKIPPED);
        assert_eq!(b.predicted_ms, None);
    }

    #[test]
    fn parse_alert() {
        let feed: Feed<AlertEntity> = serde_json::from_str(ALERT_FIXTURE).unwrap();
        let alerts = alerts_from(feed.header.timestamp, 42, &feed.entity);
        assert_eq!(alerts.len(), 1);
        let a = &alerts[0];
        assert_eq!(a.alert_id, "rs:tec:1");
        assert_eq!(a.agency_id.as_deref(), Some("tec"));
        assert_eq!(a.effect, Some(EFFECT_DETOUR));
        assert_eq!(a.route_ids, "gr:tec:H1078-23109");
        assert_eq!(a.stop_ids, "gs:tec:X");
        assert_eq!(a.header_fr.as_deref(), Some("Travaux"));
        assert_eq!(a.description_fr.as_deref(), Some("Ligne déviée"));
        assert_eq!(a.active_from, Some(1_788_732_060));
    }

    #[test]
    fn archive_brute_append_et_lit_gzip() {
        use flate2::read::MultiGzDecoder;
        use std::io::{BufRead, BufReader};

        let dir = std::env::temp_dir().join(format!("tp-raw-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let raw = RawArchive::new(&dir);

        // 1790191057 = 2026-09-23T...
        raw.append("trip-update", 1_790_191_057, r#"{"a":1}"#)
            .unwrap();
        raw.append("trip-update", 1_790_191_057, r#"{"a":2}"#)
            .unwrap();
        raw.append("alert", 1_790_191_057, r#"{"b":1}"#).unwrap();

        let path = dir
            .join(day_string(1_790_191_057))
            .join("trip-update.jsonl.gz");
        let f = std::fs::File::open(&path).unwrap();
        let lines: Vec<String> = BufReader::new(MultiGzDecoder::new(f))
            .lines()
            .map(|l| l.unwrap())
            .collect();
        assert_eq!(lines, vec![r#"{"a":1}"#, r#"{"a":2}"#]);
        assert!(
            dir.join(day_string(1_790_191_057))
                .join("alert.jsonl.gz")
                .exists()
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn purge_respecte_la_retention() {
        let dir = std::env::temp_dir().join(format!("tp-prune-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let raw = RawArchive::new(&dir).with_retention(30);

        let old = 1_790_191_057; // 2026-09-23
        let recent = old + 40 * 86_400;
        raw.append("alert", old, "{}").unwrap();
        raw.append("alert", recent, "{}").unwrap();

        let removed = raw.prune(recent).unwrap();
        assert_eq!(removed, 1);
        assert!(!dir.join(day_string(old)).exists());
        assert!(dir.join(day_string(recent)).exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn purge_desactivee_avec_retention_zero() {
        let dir = std::env::temp_dir().join(format!("tp-prune0-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let raw = RawArchive::new(&dir).with_retention(0);
        raw.append("alert", 1_790_191_057, "{}").unwrap();
        assert_eq!(raw.prune(1_790_191_057 + 999 * 86_400).unwrap(), 0);
        assert!(dir.join(day_string(1_790_191_057)).exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn fraicheur() {
        let mut s = RtState::default();
        assert!(s.is_stale(1_000, 120)); // jamais de succès -> périmé
        s.last_success = Some(950);
        assert_eq!(s.feed_age_secs(1_000), Some(50));
        assert!(!s.is_stale(1_000, 120));
        assert!(s.is_stale(1_200, 120));
    }
}
