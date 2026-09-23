//! Dépôt de données en mémoire.
//!
//! Pour l'instant alimenté par un jeu de données fictif (Liège) afin d'avoir un
//! squelette navigable. Les étapes 2 et 3 du plan remplaceront `mock()` par
//! l'index GTFS statique et le cache temps réel.

use std::collections::HashMap;

use chrono::{Duration, Utc};

use crate::domain::{
    self, Departure, Line, LineStatus, ServiceStatus, Stop, StopStatus, Thresholds, TransportMode,
};
use crate::geo::haversine_m;

pub struct Repo {
    pub stops: Vec<Stop>,
    pub statuses: HashMap<String, StopStatus>,
    pub thresholds: Thresholds,
    pub feed_age_secs: Option<u64>,
}

impl Repo {
    /// Jeu de données fictif couvrant les cinq statuts possibles.
    pub fn mock() -> Self {
        let now = Utc::now();
        let thresholds = Thresholds::default();
        let feed_age_secs = 30u64;
        let rt_fresh = true;
        let last_updated = Some(now - Duration::seconds(feed_age_secs as i64));

        let stops = vec![
            Stop {
                stop_id: "tec:liege:opera".into(),
                name: "Liège — Opéra".into(),
                lat: 50.6431,
                lon: 5.5734,
            },
            Stop {
                stop_id: "tec:liege:republique".into(),
                name: "Liège — République".into(),
                lat: 50.6410,
                lon: 5.5720,
            },
            Stop {
                stop_id: "tec:liege:guillemins".into(),
                name: "Liège — Guillemins".into(),
                lat: 50.6232,
                lon: 5.5664,
            },
        ];

        let line = |id: &str, short: &str, long: &str, mode| Line {
            route_id: id.into(),
            short_name: short.into(),
            long_name: long.into(),
            mode,
        };

        let dep = |trip: &str, headsign: &str, mins: i64, delay_secs: i64, cancelled: bool| {
            let scheduled = now + Duration::minutes(mins);
            let observed = (!cancelled).then(|| scheduled + Duration::seconds(delay_secs));
            Departure {
                trip_id: trip.into(),
                headsign: headsign.into(),
                scheduled,
                observed,
                cancelled,
            }
        };

        let l1 = line("tec:1", "1", "Gare centrale → Jemeppe", TransportMode::Bus);
        let l4 = line("tec:4", "4", "Opéra → Seraing", TransportMode::Bus);
        let l48 = line("tec:48", "48", "Guillemins → Visé", TransportMode::Bus);
        let l5 = line("tec:5", "5", "République → Herstal", TransportMode::Bus);

        let mut statuses = HashMap::new();

        statuses.insert(
            "tec:liege:opera".into(),
            build_stop_status(
                stops[0].clone(),
                vec![
                    make_line_status(
                        &l1,
                        vec![
                            dep("t1a", "Jemeppe", 5, 0, false),
                            dep("t1b", "Jemeppe", 18, 45, false),
                        ],
                        rt_fresh,
                        None,
                        thresholds,
                    ),
                    make_line_status(
                        &l4,
                        vec![dep("t4a", "Seraing", 7, 360, false)],
                        rt_fresh,
                        None,
                        thresholds,
                    ),
                    make_line_status(
                        &l48,
                        vec![
                            dep("t48a", "Visé", 10, 0, true),
                            dep("t48b", "Visé", 25, 0, false),
                        ],
                        rt_fresh,
                        None,
                        thresholds,
                    ),
                ],
                last_updated,
                Some(feed_age_secs),
            ),
        );

        statuses.insert(
            "tec:liege:republique".into(),
            build_stop_status(
                stops[1].clone(),
                vec![
                    make_line_status(
                        &l1,
                        vec![dep("t1c", "Jemeppe", 9, 0, false)],
                        rt_fresh,
                        None,
                        thresholds,
                    ),
                    make_line_status(
                        &l5,
                        vec![dep("t5a", "Herstal", 12, 0, false)],
                        rt_fresh,
                        Some("Ligne interrompue suite à un incident"),
                        thresholds,
                    ),
                ],
                last_updated,
                Some(feed_age_secs),
            ),
        );

        statuses.insert(
            "tec:liege:guillemins".into(),
            build_stop_status(
                stops[2].clone(),
                vec![
                    make_line_status(
                        &l48,
                        vec![dep("t48c", "Visé", 6, 0, false)],
                        rt_fresh,
                        None,
                        thresholds,
                    ),
                    make_line_status(
                        &l4,
                        vec![dep("t4b", "Seraing", 3, 60, false)],
                        rt_fresh,
                        None,
                        thresholds,
                    ),
                ],
                last_updated,
                Some(feed_age_secs),
            ),
        );

        Self {
            stops,
            statuses,
            thresholds,
            feed_age_secs: Some(feed_age_secs),
        }
    }

    /// Recherche textuelle sur le nom d'arrêt : insensible à la casse et aux accents.
    pub fn search(&self, q: &str) -> Vec<Stop> {
        let q = fold(q);
        if q.is_empty() {
            return Vec::new();
        }
        self.stops
            .iter()
            .filter(|s| fold(&s.name).contains(&q))
            .cloned()
            .collect()
    }

    pub fn status_for_stop(&self, stop_id: &str) -> Option<StopStatus> {
        self.statuses.get(stop_id).cloned()
    }

    /// Arrêt le plus proche dans un rayon donné (mètres).
    pub fn nearest(&self, lat: f64, lon: f64, radius_m: f64) -> Option<&Stop> {
        self.stops
            .iter()
            .map(|s| (s, haversine_m(lat, lon, s.lat, s.lon)))
            .filter(|(_, d)| *d <= radius_m)
            .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap())
            .map(|(s, _)| s)
    }
}

/// Minuscule + suppression des diacritiques français courants.
fn fold(s: &str) -> String {
    s.trim()
        .to_lowercase()
        .chars()
        .map(|c| match c {
            'à' | 'â' | 'ä' | 'á' | 'ã' => 'a',
            'é' | 'è' | 'ê' | 'ë' => 'e',
            'î' | 'ï' | 'í' => 'i',
            'ô' | 'ö' | 'ó' | 'õ' => 'o',
            'û' | 'ü' | 'ù' | 'ú' => 'u',
            'ç' => 'c',
            'ÿ' => 'y',
            other => other,
        })
        .collect()
}

fn build_stop_status(
    stop: Stop,
    lines: Vec<LineStatus>,
    last_updated: Option<chrono::DateTime<Utc>>,
    feed_age_secs: Option<u64>,
) -> StopStatus {
    // L'aide reflète la ligne la plus préoccupante.
    let worst = lines
        .iter()
        .map(|l| l.status)
        .min_by_key(|s| severity_rank(*s))
        .unwrap_or(ServiceStatus::Unknown);
    StopStatus {
        stop,
        lines,
        last_updated,
        feed_timestamp: last_updated,
        captured_at: None,
        feed_age_secs,
        refresh_age_secs: None,
        advice: domain::advice(worst).to_string(),
        served_lines: Vec::new(),
        directions: Vec::new(),
        service_note: None,
        network: None,
    }
}

/// Plus le rang est bas, plus la situation est préoccupante.
fn severity_rank(s: ServiceStatus) -> u8 {
    match s {
        ServiceStatus::Stopped => 0,
        ServiceStatus::Reduced => 1,
        ServiceStatus::Perturbed => 2,
        ServiceStatus::Unknown => 3,
        ServiceStatus::Normal => 4,
    }
}

fn make_line_status(
    line: &Line,
    departures: Vec<Departure>,
    rt_fresh: bool,
    severe_alert: Option<&str>,
    thresholds: Thresholds,
) -> LineStatus {
    let verdict = domain::evaluate(&departures, rt_fresh, severe_alert, thresholds);
    let basis = verdict.basis();
    LineStatus {
        line: line.clone(),
        status: verdict.status,
        departures,
        reason: verdict.reason,
        basis,
        evidence: verdict.evidence,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recherche_par_nom() {
        let repo = Repo::mock();
        assert_eq!(repo.search("opéra").len(), 1);
        assert_eq!(repo.search("liege").len(), 3);
        assert!(repo.search("").is_empty());
        assert!(repo.search("paris").is_empty());
    }

    #[test]
    fn statut_du_jeu_fictif_couvre_les_cas() {
        let repo = Repo::mock();
        let opera = repo.status_for_stop("tec:liege:opera").unwrap();
        assert!(
            opera
                .lines
                .iter()
                .any(|l| l.status == ServiceStatus::Perturbed)
        );
        assert!(
            opera
                .lines
                .iter()
                .any(|l| l.status == ServiceStatus::Reduced)
        );
        assert!(
            opera
                .lines
                .iter()
                .any(|l| l.status == ServiceStatus::Normal)
        );

        let republique = repo.status_for_stop("tec:liege:republique").unwrap();
        assert!(
            republique
                .lines
                .iter()
                .any(|l| l.status == ServiceStatus::Stopped)
        );
    }

    #[test]
    fn arret_le_plus_proche() {
        let repo = Repo::mock();
        let near = repo.nearest(50.6430, 5.5730, 500.0).unwrap();
        assert_eq!(near.stop_id, "tec:liege:opera");
        assert!(repo.nearest(48.85, 2.35, 500.0).is_none());
    }
}
