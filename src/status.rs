//! Service de statut : croise les horaires théoriques (GTFS statique) avec les
//! observations temps réel archivées, et produit un `StopStatus` par arrêt.

use std::collections::{HashMap, HashSet};

use anyhow::Result;
use chrono::{NaiveDateTime, TimeZone, Timelike, Utc};
use chrono_tz::Europe::Brussels;
use rusqlite::{Connection, OptionalExtension};

use crate::domain::{
    self, Departure, LineStatus, NetworkContext, ServiceStatus, StopStatus, Thresholds, evaluate,
};
use crate::gtfs::GtfsRepo;
use crate::network::{CommuneStatus, NetworkStatus, NetworkThresholds, commune_from_stop_name};

/// Fenêtre de passage affichée (minutes de service).
const HORIZON_MIN: i64 = 60;
const MAX_DEPARTURES: usize = 12;

/// Effets d'alerte GTFS-RT qui font qu'une course ne circule pas comme prévu.
const EFFECT_NO_SERVICE: i64 = 1;
const EFFECT_REDUCED_SERVICE: i64 = 2;

pub struct StatusService {
    repo: GtfsRepo,
    archive: Connection,
    thresholds: Thresholds,
    stale_after: i64,
    network: NetworkThresholds,
}

impl StatusService {
    pub fn open(
        gtfs_path: impl AsRef<std::path::Path>,
        archive_path: impl AsRef<std::path::Path>,
    ) -> Result<Self> {
        let archive = Connection::open(archive_path)?;
        // La jointure statique↔temps réel traverse deux bases : on attache le
        // GTFS à la connexion archive sous le nom `gtfs`.
        let gtfs_path = gtfs_path.as_ref();
        archive.execute(
            "ATTACH DATABASE ?1 AS gtfs",
            rusqlite::params![gtfs_path.to_string_lossy()],
        )?;
        Ok(Self {
            repo: GtfsRepo::open(gtfs_path)?,
            archive,
            thresholds: Thresholds::default(),
            stale_after: Thresholds::default().stale_feed_secs as i64,
            network: NetworkThresholds::default(),
        })
    }

    /// Heure locale Bruxelles, celle attendue par `stop_status`.
    pub fn now_brussels() -> NaiveDateTime {
        Utc::now().with_timezone(&Brussels).naive_local()
    }

    pub fn with_thresholds(mut self, thresholds: Thresholds, stale_after: i64) -> Self {
        self.thresholds = thresholds;
        self.stale_after = stale_after;
        self
    }

    /// Statut complet d'un arrêt : lignes, prochains passages, statut, aide.
    ///
    /// `now` doit être en **heure locale Bruxelles** (les horaires GTFS le sont).
    pub fn stop_status(&self, stop_id: &str, now: NaiveDateTime) -> Result<Option<StopStatus>> {
        let Some(stop) = self.repo.stop(stop_id)? else {
            return Ok(None);
        };
        // Date/heure locales (Europe/Brussels), sans dépendance TZ externe.
        let yyyymmdd = now.format("%Y%m%d").to_string();
        let now_secs =
            (now.time().hour() * 3600 + now.time().minute() * 60 + now.time().second()) as i64;
        let services = self.repo.active_services(&yyyymmdd)?;

        // Obs. RT fraîches indexées par (trip_id, stop_sequence).
        let (rt, feed_age) = self.latest_rt_for_stop(stop_id, &services)?;
        let rt_fresh = feed_age.map(|a| a <= self.stale_after).unwrap_or(false);

        // Alertes actives : lignes entièrement à l'arrêt et courses annulées.
        let alerts = self.active_alerts_for_stop(stop_id)?;

        // Passages théoriques à l'arrêt (ou sur ses quais enfants).
        let stops = self.repo.station_stops(stop_id)?;
        let mut scheduled = Vec::new();
        for s in &stops {
            scheduled.extend(self.repo.departures_at_stop(
                &s.stop_id,
                &services,
                now_secs,
                HORIZON_MIN * 60,
                MAX_DEPARTURES,
            )?);
        }
        scheduled.sort_by_key(|d| d.departure_secs);
        scheduled.truncate(MAX_DEPARTURES);

        // Regroupe par ligne.
        let mut by_route: HashMap<String, Vec<Departure>> = HashMap::new();
        for sd in scheduled {
            let key = (sd.trip_id.clone(), sd.stop_sequence);
            let rt_hit = rt.get(&key);
            // Annulée si : arrêt sauté (RT) OU course ciblée par une alerte.
            let cancelled = rt_hit
                .map(|o| o.schedule_relationship == 1)
                .unwrap_or(false)
                || alerts.cancelled_trips.contains(&sd.trip_id);
            let observed = rt_hit
                .and_then(|o| o.predicted_ms)
                .and_then(|ms| Utc.timestamp_millis_opt(ms).single());
            let scheduled_dt = day_secs_to_utc(now, sd.departure_secs);
            by_route
                .entry(sd.route_id.clone())
                .or_default()
                .push(Departure {
                    trip_id: sd.trip_id,
                    headsign: sd.headsign,
                    scheduled: scheduled_dt,
                    observed,
                    cancelled,
                });
        }

        let mut lines = Vec::new();
        for (route_id, departures) in by_route {
            let Some(route) = self.repo.route(&route_id)? else {
                continue;
            };
            // Alerte sévère au niveau ligne (ex. service interrompu).
            let severe = alerts.no_service_routes.get(&route_id).map(String::as_str);
            let verdict = evaluate(&departures, rt_fresh, severe, self.thresholds);
            let basis = verdict.basis();
            lines.push(LineStatus {
                line: route,
                status: verdict.status,
                departures,
                reason: verdict.reason,
                basis,
                evidence: verdict.evidence,
            });
        }
        lines.sort_by(|a, b| a.line.short_name.cmp(&b.line.short_name));

        let worst = lines
            .iter()
            .map(|l| l.status)
            .min_by_key(|s| severity_rank(*s))
            .unwrap_or(ServiceStatus::Unknown);

        // Contexte systémique de la commune de l'arrêt.
        let network = self.network_context(&stop.name)?;

        Ok(Some(StopStatus {
            stop,
            lines,
            last_updated: feed_age.map(|a| Utc::now() - chrono::Duration::seconds(a)),
            feed_age_secs: feed_age.map(|a| a.max(0) as u64),
            advice: domain::advice(worst).to_string(),
            network,
        }))
    }

    /// Dernières observations RT pour les `trip_id` desservant l'arrêt.
    fn latest_rt_for_stop(
        &self,
        stop_id: &str,
        services: &[String],
    ) -> Result<(RtIndex, Option<i64>)> {
        if services.is_empty() {
            return Ok((HashMap::new(), None));
        }
        let placeholders = (0..services.len())
            .map(|i| format!("?{}", i + 2))
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT o.trip_id, o.stop_sequence, o.predicted_ms, o.schedule_relationship, o.feed_ts
             FROM main.rt_observations o
             JOIN gtfs.gtfs_trips t ON t.trip_id = o.trip_id
             JOIN gtfs.gtfs_stop_times st ON st.trip_id = o.trip_id AND st.stop_sequence = o.stop_sequence
             WHERE st.stop_id = ?1 AND t.service_id IN ({placeholders})
               AND o.feed_ts = (SELECT MAX(feed_ts) FROM main.rt_observations)"
        );
        let mut stmt = self.archive.prepare(&sql)?;
        let mut binds: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(stop_id.to_string())];
        for s in services {
            binds.push(Box::new(s.clone()));
        }
        let params_ref: Vec<&dyn rusqlite::ToSql> = binds.iter().map(|b| b.as_ref()).collect();

        let mut map = HashMap::new();
        let mut feed_ts = None;
        let mut rows = stmt.query(params_ref.as_slice())?;
        while let Some(r) = rows.next()? {
            let trip_id: String = r.get(0)?;
            let seq: i64 = r.get(1)?;
            feed_ts = Some(r.get::<_, i64>(4)?);
            map.insert(
                (trip_id, seq),
                RtObs {
                    predicted_ms: r.get(2)?,
                    schedule_relationship: r.get(3)?,
                },
            );
        }

        // Âge du feed = maintenant - feed_ts observé.
        let age = match feed_ts {
            Some(ts) => Some((Utc::now().timestamp() - ts).max(0)),
            None => self.latest_feed_age()?,
        };
        Ok((map, age))
    }

    fn latest_feed_age(&self) -> Result<Option<i64>> {
        let ts: Option<i64> =
            self.archive
                .query_row("SELECT MAX(feed_ts) FROM rt_observations", [], |r| r.get(0))?;
        Ok(ts.map(|t| (Utc::now().timestamp() - t).max(0)))
    }

    /// Alertes actives (dernier feed) concernant l'arrêt : routes entièrement
    /// à l'arrêt (`effect=NO_SERVICE` sans course ciblée) et courses annulées.
    fn active_alerts_for_stop(&self, stop_id: &str) -> Result<ActiveAlerts> {
        let mut out = ActiveAlerts::default();

        // 1) Annulations ciblant une course précise, dont cette course dessert
        //    l'arrêt (ou l'un de ses quais enfants).
        let no_svc = EFFECT_NO_SERVICE.to_string();
        let reduced = EFFECT_REDUCED_SERVICE.to_string();
        let mut stmt = self.archive.prepare(
            "SELECT DISTINCT at.trip_id, a.route_ids, a.header_fr
             FROM main.rt_alert_trips at
             JOIN main.rt_alerts a ON a.alert_id = at.alert_id AND a.feed_ts = at.feed_ts
             JOIN gtfs.gtfs_stop_times st ON st.trip_id = at.trip_id
             WHERE st.stop_id = ?1
               AND at.feed_ts = (SELECT MAX(feed_ts) FROM main.rt_alerts)
               AND a.effect IN (?2, ?3)",
        )?;
        let rows = stmt.query_map(rusqlite::params![stop_id, no_svc, reduced], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, Option<String>>(1)?,
                r.get::<_, Option<String>>(2)?,
            ))
        })?;
        for row in rows {
            let (trip_id, route_ids, header) = row?;
            out.cancelled_trips.insert(trip_id);
            let _ = (route_ids, header);
        }

        // 2) Alertes NO_SERVICE **sans course ciblée** = interruption de toute la
        //    ligne. Celles qui ciblent des courses précises (cas courant chez TEC,
        //    ex. « Annulations ») ne doivent PAS être lues comme « ligne à l'arrêt » :
        //    seules les courses concernées le sont (traitées au point 1).
        let mut stmt = self.archive.prepare(
            "SELECT route_ids, header_fr, description_fr
             FROM main.rt_alerts a
             WHERE a.feed_ts = (SELECT MAX(feed_ts) FROM main.rt_alerts)
               AND a.effect = ?1 AND a.route_ids <> ''
               AND NOT EXISTS (
                   SELECT 1 FROM main.rt_alert_trips at
                   WHERE at.alert_id = a.alert_id AND at.feed_ts = a.feed_ts)",
        )?;
        let rows = stmt.query_map([EFFECT_NO_SERVICE], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, Option<String>>(1)?,
                r.get::<_, Option<String>>(2)?,
            ))
        })?;
        for row in rows {
            let (route_ids, header, desc) = row?;
            let msg = header
                .or(desc)
                .unwrap_or_else(|| "Service interrompu".into());
            for rid in route_ids.split(',') {
                if !rid.is_empty() {
                    out.no_service_routes
                        .entry(rid.to_string())
                        .or_insert_with(|| msg.clone());
                }
            }
        }

        Ok(out)
    }

    /// Contexte systémique pour la commune d'un arrêt : part de courses
    /// annulées, calculée sur la même base attachée.
    fn network_context(&self, stop_name: &str) -> Result<Option<NetworkContext>> {
        let Some(commune) = commune_from_stop_name(stop_name) else {
            return Ok(None);
        };
        let status = self.commune_stats(&commune)?;
        Ok(status.map(|c| NetworkContext {
            commune: c.commune,
            status: match c.status {
                NetworkStatus::Normal => "normal",
                NetworkStatus::Degraded => "degraded",
                NetworkStatus::Critical => "critical",
            }
            .to_string(),
            status_label: c.status_label,
            cancelled_ratio: c.cancelled_ratio,
            trips_cancelled: c.trips_cancelled,
            trips_scheduled: c.trips_scheduled,
        }))
    }

    /// Compteurs d'annulation pour une commune, lus depuis la table
    /// matérialisée `network_stats` (reconstruite à chaque cycle RT).
    fn commune_stats(&self, commune: &str) -> Result<Option<CommuneStatus>> {
        let row = self
            .archive
            .query_row(
                "SELECT trips_scheduled, trips_cancelled FROM main.network_stats
                 WHERE upper(commune) = upper(?1)",
                [commune],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)),
            )
            .optional()?;
        let Some((sched, ann)) = row else {
            return Ok(None);
        };
        // Recalcule le statut via les seuils réseau.
        Ok(Some(derive_commune_status(
            CommuneStatus {
                commune: commune.to_string(),
                trips_scheduled: sched,
                trips_cancelled: ann,
                cancelled_ratio: if sched == 0 {
                    0.0
                } else {
                    ann as f64 / sched as f64
                },
                status: NetworkStatus::Normal,
                status_label: String::new(),
            },
            self.network,
        )))
    }
}

/// Applique les seuils réseau à des compteurs déjà calculés.
fn derive_commune_status(mut c: CommuneStatus, th: NetworkThresholds) -> CommuneStatus {
    let st = if c.trips_scheduled == 0 {
        NetworkStatus::Normal
    } else if c.trips_cancelled >= th.min_annulled && c.cancelled_ratio >= th.critical_ratio {
        NetworkStatus::Critical
    } else if c.trips_cancelled >= th.min_annulled && c.cancelled_ratio >= th.degraded_ratio {
        NetworkStatus::Degraded
    } else {
        NetworkStatus::Normal
    };
    c.status = st;
    c.status_label = st.label().to_string();
    c
}

/// Alertes actives résumées pour un arrêt.
#[derive(Debug, Default)]
struct ActiveAlerts {
    /// `route_id` entièrement à l'arrêt -> message.
    no_service_routes: HashMap<String, String>,
    /// `trip_id` explicitement annulés par une alerte.
    cancelled_trips: HashSet<String>,
}

/// Observations RT indexées par `(trip_id, stop_sequence)`.
type RtIndex = HashMap<(String, i64), RtObs>;

#[derive(Debug, Clone, Copy)]
struct RtObs {
    predicted_ms: Option<i64>,
    schedule_relationship: i64,
}

/// Convertit un horaire GTFS (secondes depuis minuit, heure locale Bruxelles)
/// en instant UTC, pour la date de service `now`.
fn day_secs_to_utc(now: NaiveDateTime, secs: i64) -> chrono::DateTime<Utc> {
    let date = now.date();
    let naive = date.and_hms_opt(0, 0, 0).unwrap() + chrono::Duration::seconds(secs);
    // Heure locale Bruxelles -> UTC (gère l'heure d'été/hiver).
    match Brussels.from_local_datetime(&naive).earliest() {
        Some(dt) => dt.with_timezone(&Utc),
        None => Utc.from_utc_datetime(&naive), // heure ambiguë (passage DST)
    }
}

fn severity_rank(s: ServiceStatus) -> u8 {
    match s {
        ServiceStatus::Stopped => 0,
        ServiceStatus::Reduced => 1,
        ServiceStatus::Perturbed => 2,
        ServiceStatus::Unknown => 3,
        ServiceStatus::Normal => 4,
    }
}

/// Récupère le jour de service courant au format `YYYYMMDD` (utile aux tests).
pub fn service_date(now: NaiveDateTime) -> String {
    now.format("%Y%m%d").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::{AlertRecord, AlertTrip, Archive, Observation};
    use crate::gtfs::GtfsRepo;
    use chrono::NaiveDate;

    /// Construit un couple GTFS+archive temporaire pour tester le croisement.
    ///
    /// GTFS : arrêt A desservi par la ligne r1 via 2 courses (t1, t2) à 10:00 et 10:30.
    /// Archive : une alerte NO_SERVICE ciblant **t1 uniquement** (annulation partielle),
    /// avec observation fraîche.
    fn fixture(dir: &std::path::Path, now_secs: i64) -> StatusService {
        let gtfs_path = dir.join("gtfs.sqlite");
        let archive_path = dir.join("archive.sqlite");

        // --- GTFS ---
        let g = GtfsRepo::create_schema_at(&gtfs_path).unwrap();
        g.execute(
            "INSERT INTO gtfs_stops (stop_id, stop_name, lat, lon) VALUES ('A', 'LIEGE Test', 50.64, 5.57)",
            [],
        )
        .unwrap();
        g.execute(
            "INSERT INTO gtfs_routes (route_id, short_name, long_name, route_type)
             VALUES ('gr:tec:L0001', '1', 'Test', 3)",
            [],
        )
        .unwrap();
        g.execute(
            "INSERT INTO gtfs_trips (trip_id, route_id, service_id, headsign)
             VALUES ('t1', 'gr:tec:L0001', 'svc', 'Direc'), ('t2', 'gr:tec:L0001', 'svc', 'Direc')",
            [],
        )
        .unwrap();
        // Deux passages à 10:00 (t1) et 10:30 (t2), arrêt A.
        g.execute(
            "INSERT INTO gtfs_stop_times (trip_id, stop_sequence, stop_id, departure_secs)
             VALUES ('t1', 1, 'A', 36000), ('t2', 1, 'A', 37800)",
            [],
        )
        .unwrap();
        // Service actif aujourd'hui (tous les jours).
        let today = chrono::Utc::now()
            .with_timezone(&Brussels)
            .format("%Y%m%d")
            .to_string();
        g.execute(
            "INSERT INTO gtfs_calendar
             (service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, start_date, end_date)
             VALUES ('svc', 1,1,1,1,1,1,1, '20200101', '20301231')",
            [],
        )
        .unwrap();
        drop(g);
        let _ = today;

        // --- Archive RT ---
        let mut a = Archive::open(&archive_path).unwrap();
        let feed_ts = chrono::Utc::now().timestamp();
        // Observation fraîche pour t1 (annulée) et t2 (à l'heure).
        a.insert_observations(&[
            Observation {
                feed_ts,
                captured_at: feed_ts,
                trip_id: "t1".into(),
                route_id: Some("gr:tec:L0001".into()),
                stop_id: "A".into(),
                stop_sequence: 1,
                scheduled_ms: None,
                predicted_ms: None,
                delay_s: None,
                schedule_relationship: 1, // SKIPPED
            },
            Observation {
                feed_ts,
                captured_at: feed_ts,
                trip_id: "t2".into(),
                route_id: Some("gr:tec:L0001".into()),
                stop_id: "A".into(),
                stop_sequence: 1,
                scheduled_ms: Some(37_800_000),
                predicted_ms: Some(37_860_000),
                delay_s: Some(60),
                schedule_relationship: 0,
            },
        ])
        .unwrap();
        a.insert_alerts(&[AlertRecord {
            feed_ts,
            captured_at: feed_ts,
            alert_id: "rs:tec:1".into(),
            agency_id: Some("tec".into()),
            effect: Some(1), // NO_SERVICE
            severity: Some(2),
            cause: Some(2),
            route_ids: "gr:tec:L0001".into(),
            stop_ids: String::new(),
            header_fr: Some("Annulations".into()),
            description_fr: Some("Annulation voyage".into()),
            active_from: None,
            active_to: None,
        }])
        .unwrap();
        // L'alerte cible t1, PAS la ligne entière.
        a.insert_alert_trips(&[AlertTrip {
            feed_ts,
            alert_id: "rs:tec:1".into(),
            trip_id: "t1".into(),
            start_date: None,
        }])
        .unwrap();
        drop(a);

        let _ = now_secs;
        StatusService::open(&gtfs_path, &archive_path).unwrap()
    }

    #[test]
    fn alerte_ciblant_une_course_ne_rend_pas_la_ligne_arretee() {
        let dir = tempfile::tempdir().unwrap();
        let svc = fixture(dir.path(), 0);
        // 09:55 locale : les deux passages (10:00, 10:30) sont dans la fenêtre.
        let now = NaiveDate::from_ymd_opt(2026, 9, 23)
            .unwrap()
            .and_hms_opt(9, 55, 0)
            .unwrap();
        let st = svc.stop_status("A", now).unwrap().unwrap();
        assert_eq!(st.lines.len(), 1, "une seule ligne");

        let line = &st.lines[0];
        // La ligne n'est PAS à l'arrêt : seule une course sur deux est annulée.
        assert_ne!(
            line.status,
            ServiceStatus::Stopped,
            "ligne ne doit pas être stoppée"
        );
        // Et la preuve doit mentionner l'annulation ciblée.
        let cancelled = line
            .evidence
            .iter()
            .any(|e| e.kind == crate::domain::EvidenceKind::Cancelled);
        assert!(cancelled, "preuve d'annulation attendue");
        // La course t1 est marquée annulée ; t2 ne l'est pas.
        let t1 = line.departures.iter().find(|d| d.trip_id == "t1").unwrap();
        let t2 = line.departures.iter().find(|d| d.trip_id == "t2").unwrap();
        assert!(t1.cancelled, "t1 doit être annulée");
        assert!(!t2.cancelled, "t2 ne doit pas être annulée");
    }

    #[test]
    fn date_de_service() {
        let d = chrono::NaiveDate::from_ymd_opt(2026, 9, 23)
            .unwrap()
            .and_hms_opt(8, 30, 0)
            .unwrap();
        assert_eq!(service_date(d), "20260923");
        assert_eq!(
            crate::gtfs::weekday_column(&service_date(d)).unwrap(),
            "wednesday"
        );
    }

    #[test]
    fn conversion_secondes_vers_utc_gere_heure_ete() {
        // 23/09/2026 : Bruxelles est en heure d'été (UTC+2).
        // 05:07 local -> 03:07 UTC.
        let now = chrono::NaiveDate::from_ymd_opt(2026, 9, 23)
            .unwrap()
            .and_hms_opt(8, 0, 0)
            .unwrap();
        let dt = day_secs_to_utc(now, 5 * 3600 + 7 * 60);
        assert_eq!(dt.format("%H:%M").to_string(), "03:07");
    }

    #[test]
    fn conversion_secondes_vers_utc_gere_heure_hiver() {
        // 15/01/2026 : Bruxelles en heure d'hiver (UTC+1). 05:07 -> 04:07 UTC.
        let now = chrono::NaiveDate::from_ymd_opt(2026, 1, 15)
            .unwrap()
            .and_hms_opt(8, 0, 0)
            .unwrap();
        let dt = day_secs_to_utc(now, 5 * 3600 + 7 * 60);
        assert_eq!(dt.format("%H:%M").to_string(), "04:07");
    }
}
