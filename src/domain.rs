//! Domaine pur de TransitPulse : types et moteur de statut.
//!
//! Aucune I/O ici. C'est le cœur du produit : décider si, pour un arrêt et une
//! ligne donnés, le service est normal, perturbé, réduit, à l'arrêt ou inconnu.

use chrono::{DateTime, Utc};
use serde::Serialize;

/// Statut de fiabilité d'une ligne à un arrêt, maintenant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceStatus {
    /// Service prévu, temps réel à l'heure, aucune alerte.
    Normal,
    /// Retards au-delà du seuil.
    Perturbed,
    /// Part importante de passages annulés/sautés.
    Reduced,
    /// Plus aucun passage prévu, tout annulé, ou alerte sévère.
    Stopped,
    /// Données temps réel absentes ou trop anciennes : on ne peut pas affirmer.
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TransportMode {
    Bus,
    Tram,
    Metro,
    Train,
}

#[derive(Debug, Clone, Serialize)]
pub struct Stop {
    pub stop_id: String,
    pub name: String,
    pub lat: f64,
    pub lon: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct Line {
    pub route_id: String,
    pub short_name: String,
    pub long_name: String,
    pub mode: TransportMode,
}

/// Un passage à un arrêt, théorique et (éventuellement) observé.
#[derive(Debug, Clone, Serialize)]
pub struct Departure {
    pub trip_id: String,
    pub headsign: String,
    pub scheduled: DateTime<Utc>,
    pub observed: Option<DateTime<Utc>>,
    pub cancelled: bool,
}

impl Departure {
    /// Retard observé en secondes (positif = en retard). `None` si pas d'observé.
    pub fn delay_secs(&self) -> Option<i64> {
        self.observed.map(|o| (o - self.scheduled).num_seconds())
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct LineStatus {
    pub line: Line,
    pub status: ServiceStatus,
    pub departures: Vec<Departure>,
    pub reason: Option<String>,
}

/// Réponse complète pour un arrêt : toutes les lignes qui devraient y passer.
#[derive(Debug, Clone, Serialize)]
pub struct StopStatus {
    pub stop: Stop,
    pub lines: Vec<LineStatus>,
    pub last_updated: Option<DateTime<Utc>>,
    pub feed_age_secs: Option<u64>,
    pub advice: String,
}

/// Seuils configurables du moteur de statut.
#[derive(Debug, Clone, Copy)]
pub struct Thresholds {
    /// Retard (s) à partir duquel on considère la ligne perturbée.
    pub late_secs: i64,
    /// Part de passages annulés à partir de laquelle le service est « réduit ».
    pub reduced_cancelled_ratio: f64,
    /// Âge (s) au-delà duquel le flux temps réel est considéré comme périmé.
    pub stale_feed_secs: u64,
}

impl Default for Thresholds {
    fn default() -> Self {
        Self {
            late_secs: 180,
            reduced_cancelled_ratio: 0.4,
            stale_feed_secs: 120,
        }
    }
}

/// Décide du statut d'une ligne à partir de ses prochains passages.
///
/// `rt_fresh` indique si l'on dispose de données temps réel récentes pour cette
/// ligne. `severe_alert` porte un message si une alerte majeure couvre la ligne.
///
/// Règle d'or : sans données temps réel fraîches **et** avec du service prévu,
/// on répond `Unknown` — jamais `Stopped`. Un trou de données n'est pas une panne.
pub fn evaluate(
    departures: &[Departure],
    rt_fresh: bool,
    severe_alert: Option<&str>,
    thresholds: Thresholds,
) -> (ServiceStatus, Option<String>) {
    if departures.is_empty() {
        return (
            ServiceStatus::Stopped,
            Some("Aucun passage prévu à cet arrêt".to_string()),
        );
    }

    if !rt_fresh {
        return (
            ServiceStatus::Unknown,
            Some("Données temps réel indisponibles ou trop anciennes".to_string()),
        );
    }

    if let Some(msg) = severe_alert {
        return (ServiceStatus::Stopped, Some(msg.to_string()));
    }

    let total = departures.len() as f64;
    let cancelled = departures.iter().filter(|d| d.cancelled).count() as f64;
    let ratio = cancelled / total;

    if ratio >= 1.0 {
        return (
            ServiceStatus::Stopped,
            Some("Tous les passages sont annulés".to_string()),
        );
    }

    if ratio >= thresholds.reduced_cancelled_ratio {
        return (
            ServiceStatus::Reduced,
            Some(format!(
                "{} % des passages annulés",
                (ratio * 100.0).round()
            )),
        );
    }

    let max_delay = departures
        .iter()
        .filter(|d| !d.cancelled)
        .filter_map(|d| d.delay_secs())
        .max()
        .unwrap_or(0);

    if max_delay >= thresholds.late_secs {
        return (
            ServiceStatus::Perturbed,
            Some(format!("Retard jusqu'à {} min", max_delay / 60)),
        );
    }

    (ServiceStatus::Normal, None)
}

/// Phrase d'aide à la décision associée à un statut.
pub fn advice(status: ServiceStatus) -> &'static str {
    match status {
        ServiceStatus::Normal => "Vous pouvez compter sur ce bus.",
        ServiceStatus::Perturbed => "Retard en cours : prévoyez plus large.",
        ServiceStatus::Reduced => "Service fortement réduit : prévoyez un plan B.",
        ServiceStatus::Stopped => "Service à l'arrêt : cherchez une alternative.",
        ServiceStatus::Unknown => "Données manquantes : vérifiez un arrêt voisin.",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn dep(mins: i64, delay_secs: i64, cancelled: bool) -> Departure {
        let scheduled = Utc::now() + Duration::minutes(mins);
        let observed = (!cancelled).then(|| scheduled + Duration::seconds(delay_secs));
        Departure {
            trip_id: format!("t{mins}"),
            headsign: "Test".into(),
            scheduled,
            observed,
            cancelled,
        }
    }

    #[test]
    fn a_lheure_est_normal() {
        let deps = vec![dep(5, 0, false), dep(20, 30, false)];
        let (s, _) = evaluate(&deps, true, None, Thresholds::default());
        assert_eq!(s, ServiceStatus::Normal);
    }

    #[test]
    fn retard_au_dela_du_seuil_est_perturbe() {
        let deps = vec![dep(5, 300, false)];
        let (s, r) = evaluate(&deps, true, None, Thresholds::default());
        assert_eq!(s, ServiceStatus::Perturbed);
        assert!(r.unwrap().contains("5 min"));
    }

    #[test]
    fn moitie_annulee_est_reduit() {
        let deps = vec![dep(5, 0, true), dep(20, 0, false)];
        let (s, _) = evaluate(&deps, true, None, Thresholds::default());
        assert_eq!(s, ServiceStatus::Reduced);
    }

    #[test]
    fn tout_annule_est_arrete() {
        let deps = vec![dep(5, 0, true), dep(20, 0, true)];
        let (s, _) = evaluate(&deps, true, None, Thresholds::default());
        assert_eq!(s, ServiceStatus::Stopped);
    }

    #[test]
    fn aucun_passage_est_arrete() {
        let (s, _) = evaluate(&[], true, None, Thresholds::default());
        assert_eq!(s, ServiceStatus::Stopped);
    }

    #[test]
    fn alerte_severe_est_arrete() {
        let deps = vec![dep(5, 0, false)];
        let (s, r) = evaluate(
            &deps,
            true,
            Some("Ligne interrompue"),
            Thresholds::default(),
        );
        assert_eq!(s, ServiceStatus::Stopped);
        assert_eq!(r.unwrap(), "Ligne interrompue");
    }

    #[test]
    fn sans_rt_frais_et_service_prevu_est_inconnu() {
        let deps = vec![dep(5, 0, false)];
        let (s, _) = evaluate(&deps, false, None, Thresholds::default());
        assert_eq!(s, ServiceStatus::Unknown);
    }

    #[test]
    fn sans_rt_frais_et_aucun_service_reste_arrete() {
        // Le statique est fiable : pas de service prévu -> arrêt, même sans RT.
        let (s, _) = evaluate(&[], false, None, Thresholds::default());
        assert_eq!(s, ServiceStatus::Stopped);
    }
}
