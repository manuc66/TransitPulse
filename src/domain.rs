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
    /// Preuves qui justifient le statut (retards observés, annulations, alerte,
    /// fraîcheur des données…). Rend le verdict vérifiable, qu'il soit rassurant
    /// ou alarmant.
    pub evidence: Vec<Evidence>,
    /// Phrase courte résumant sur quoi repose le statut.
    pub basis: String,
}

/// Nature d'une preuve appuyant un statut.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceKind {
    /// Données temps réel fraîches disponibles.
    DataFresh,
    /// Données temps réel absentes ou périmées.
    DataStale,
    /// Alerte de service (interruption annoncée).
    NoServiceAlert,
    /// Passages annulés.
    Cancelled,
    /// Retards observés.
    Delay,
    /// Passages à l'heure (observés).
    OnTime,
    /// Aucun passage prévu (fin de service / ligne non desservante).
    NoScheduledService,
}

/// Preuve élémentaire justifiant (ou nuançant) un statut.
#[derive(Debug, Clone, Serialize)]
pub struct Evidence {
    pub kind: EvidenceKind,
    pub detail: String,
}

impl Evidence {
    pub fn new(kind: EvidenceKind, detail: impl Into<String>) -> Self {
        Self {
            kind,
            detail: detail.into(),
        }
    }
}

/// Réponse complète pour un arrêt : toutes les lignes qui devraient y passer.
#[derive(Debug, Clone, Serialize)]
pub struct StopStatus {
    pub stop: Stop,
    /// Lignes desservant l'arrêt **dans la fenêtre courante** (avec passages).
    pub lines: Vec<LineStatus>,
    /// Toutes les lignes qui desservent habituellement cet arrêt, même hors
    /// service (ex. « quelles lignes passent ici ? » à 23 h). Évite un écran vide.
    pub served_lines: Vec<Line>,
    /// Sens de passage (ligne + destination), même hors service : permet de
    /// distinguer deux quais de même nom et de savoir où l'on va.
    pub directions: Vec<crate::gtfs::Direction>,
    /// Heure à laquelle le flux officiel a produit sa dernière mise à jour
    /// (timestamp du feed GTFS-RT). « Quelle est la fraîcheur de la donnée ? »
    #[serde(skip_serializing_if = "Option::is_none")]
    pub feed_timestamp: Option<DateTime<Utc>>,
    /// Heure à laquelle nous avons interrogé et archivé le flux.
    /// « Quand a-t-on rafraîchi pour la dernière fois ? »
    #[serde(skip_serializing_if = "Option::is_none")]
    pub captured_at: Option<DateTime<Utc>>,
    /// Âge de la donnée officielle (maintenant − `feed_timestamp`).
    pub feed_age_secs: Option<u64>,
    /// Temps écoulé depuis notre dernier rafraîchissement (maintenant − `captured_at`).
    pub refresh_age_secs: Option<u64>,
    /// Compat : ancien champ conservé pour les clients existants.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_updated: Option<DateTime<Utc>>,
    pub advice: String,
    /// Précision temporelle quand aucune ligne n'a de passage imminent
    /// (ex. « service terminé, dernier passage à 20:12 »).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_note: Option<String>,
    /// Contexte systémique : état du réseau dans la commune de l'arrêt,
    /// pour distinguer « mon bus est en retard » de « tout le réseau est à l'arrêt ».
    #[serde(skip_serializing_if = "Option::is_none")]
    pub network: Option<NetworkContext>,
}

/// Résumé de l'état réseau local, vu depuis un arrêt.
#[derive(Debug, Clone, Serialize)]
pub struct NetworkContext {
    pub commune: String,
    pub status: String,
    pub status_label: String,
    pub cancelled_ratio: f64,
    pub trips_cancelled: i64,
    pub trips_scheduled: i64,
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

/// Verdict du moteur : statut, raison courte, et preuves détaillées.
#[derive(Debug, Clone)]
pub struct Verdict {
    pub status: ServiceStatus,
    pub reason: Option<String>,
    pub evidence: Vec<Evidence>,
}

impl Verdict {
    /// Phrase résumant sur quoi le statut repose.
    pub fn basis(&self) -> String {
        if self.evidence.is_empty() {
            return "Aucune donnée exploitable".to_string();
        }
        self.evidence
            .iter()
            .map(|e| e.detail.clone())
            .collect::<Vec<_>>()
            .join(" ; ")
    }
}

/// Décide du statut d'une ligne à partir de ses prochains passages, **et**
/// documente les preuves qui l'étayent.
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
) -> Verdict {
    if departures.is_empty() {
        return Verdict {
            status: ServiceStatus::Stopped,
            reason: Some("Aucun passage prévu à cet arrêt".to_string()),
            evidence: vec![Evidence::new(
                EvidenceKind::NoScheduledService,
                "aucun passage prévu dans l'heure à venir",
            )],
        };
    }

    let total = departures.len();
    let cancelled = departures.iter().filter(|d| d.cancelled).count();
    let observed: Vec<i64> = departures
        .iter()
        .filter(|d| !d.cancelled)
        .filter_map(|d| d.delay_secs())
        .collect();
    let delays_detail = || {
        observed
            .iter()
            .map(|s| format!("{:+} min", s / 60))
            .collect::<Vec<_>>()
            .join(", ")
    };

    if !rt_fresh {
        return Verdict {
            status: ServiceStatus::Unknown,
            reason: Some("Données temps réel indisponibles ou trop anciennes".to_string()),
            evidence: vec![
                Evidence::new(EvidenceKind::DataStale, "aucune donnée temps réel fraîche"),
                Evidence::new(
                    EvidenceKind::NoScheduledService,
                    format!("{total} passage(s) prévu(s) au horaire théorique"),
                ),
            ],
        };
    }

    if let Some(msg) = severe_alert {
        return Verdict {
            status: ServiceStatus::Stopped,
            reason: Some(msg.to_string()),
            evidence: vec![
                Evidence::new(EvidenceKind::NoServiceAlert, format!("alerte : {msg}")),
                Evidence::new(
                    EvidenceKind::DataFresh,
                    "flux temps réel à jour au moment du verdict",
                ),
            ],
        };
    }

    let ratio = cancelled as f64 / total as f64;

    if ratio >= 1.0 {
        return Verdict {
            status: ServiceStatus::Stopped,
            reason: Some("Tous les passages sont annulés".to_string()),
            evidence: vec![
                Evidence::new(
                    EvidenceKind::Cancelled,
                    format!("{cancelled}/{total} passages annulés"),
                ),
                Evidence::new(EvidenceKind::DataFresh, "données temps réel fraîches"),
            ],
        };
    }

    if ratio >= thresholds.reduced_cancelled_ratio {
        return Verdict {
            status: ServiceStatus::Reduced,
            reason: Some(format!(
                "{} % des passages annulés",
                (ratio * 100.0).round()
            )),
            evidence: vec![
                Evidence::new(
                    EvidenceKind::Cancelled,
                    format!("{cancelled}/{total} passages annulés"),
                ),
                Evidence::new(EvidenceKind::DataFresh, "données temps réel fraîches"),
            ],
        };
    }

    let max_delay = observed.iter().copied().max().unwrap_or(0);

    if max_delay >= thresholds.late_secs {
        return Verdict {
            status: ServiceStatus::Perturbed,
            reason: Some(format!("Retard jusqu'à {} min", max_delay / 60)),
            evidence: vec![
                Evidence::new(
                    EvidenceKind::Delay,
                    format!("retards observés : {}", delays_detail()),
                ),
                Evidence::new(EvidenceKind::DataFresh, "données temps réel fraîches"),
            ],
        };
    }

    // Normal : on montrer explicitement ce qui rassure (preuves positives).
    let mut evidence = vec![Evidence::new(
        EvidenceKind::DataFresh,
        "données temps réel fraîches",
    )];
    if observed.is_empty() {
        evidence.push(Evidence::new(
            EvidenceKind::NoScheduledService,
            "passages au horaire théorique (pas encore de prédiction temps réel)",
        ));
    } else {
        evidence.push(Evidence::new(
            EvidenceKind::OnTime,
            if observed.iter().all(|s| *s == 0) {
                "passages observés à l'heure".to_string()
            } else {
                format!("écarts dans les limites : {}", delays_detail())
            },
        ));
    }
    if cancelled > 0 {
        evidence.push(Evidence::new(
            EvidenceKind::Cancelled,
            format!("{cancelled}/{total} annulé(s), sous le seuil"),
        ));
    }
    Verdict {
        status: ServiceStatus::Normal,
        reason: None,
        evidence,
    }
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

    fn has_evidence(v: &Verdict, kind: EvidenceKind) -> bool {
        v.evidence.iter().any(|e| e.kind == kind)
    }

    #[test]
    fn a_lheure_est_normal_avec_preuve() {
        let deps = vec![dep(5, 0, false), dep(20, 30, false)];
        let v = evaluate(&deps, true, None, Thresholds::default());
        assert_eq!(v.status, ServiceStatus::Normal);
        // Un verdict « normal » s'appuie sur une preuve explicite.
        assert!(has_evidence(&v, EvidenceKind::DataFresh));
        assert!(has_evidence(&v, EvidenceKind::OnTime));
        assert!(v.basis().contains("temps réel"));
    }

    #[test]
    fn retard_au_dela_du_seuil_est_perturbe() {
        let deps = vec![dep(5, 300, false)];
        let v = evaluate(&deps, true, None, Thresholds::default());
        assert_eq!(v.status, ServiceStatus::Perturbed);
        assert!(v.reason.as_deref().unwrap().contains("5 min"));
        assert!(has_evidence(&v, EvidenceKind::Delay));
    }

    #[test]
    fn moitie_annulee_est_reduit() {
        let deps = vec![dep(5, 0, true), dep(20, 0, false)];
        let v = evaluate(&deps, true, None, Thresholds::default());
        assert_eq!(v.status, ServiceStatus::Reduced);
        assert!(has_evidence(&v, EvidenceKind::Cancelled));
    }

    #[test]
    fn tout_annule_est_arrete() {
        let deps = vec![dep(5, 0, true), dep(20, 0, true)];
        let v = evaluate(&deps, true, None, Thresholds::default());
        assert_eq!(v.status, ServiceStatus::Stopped);
        assert!(v.evidence.iter().any(|e| e.detail.contains("2/2")));
    }

    #[test]
    fn aucun_passage_est_arrete_avec_preuve() {
        let v = evaluate(&[], true, None, Thresholds::default());
        assert_eq!(v.status, ServiceStatus::Stopped);
        assert!(has_evidence(&v, EvidenceKind::NoScheduledService));
    }

    #[test]
    fn alerte_severe_est_arrete() {
        let deps = vec![dep(5, 0, false)];
        let v = evaluate(
            &deps,
            true,
            Some("Ligne interrompue"),
            Thresholds::default(),
        );
        assert_eq!(v.status, ServiceStatus::Stopped);
        assert_eq!(v.reason.as_deref(), Some("Ligne interrompue"));
        assert!(has_evidence(&v, EvidenceKind::NoServiceAlert));
    }

    #[test]
    fn sans_rt_frais_et_service_prevu_est_inconnu() {
        let deps = vec![dep(5, 0, false)];
        let v = evaluate(&deps, false, None, Thresholds::default());
        assert_eq!(v.status, ServiceStatus::Unknown);
        assert!(has_evidence(&v, EvidenceKind::DataStale));
    }

    #[test]
    fn normal_sans_prediction_explique_le_theorique() {
        // Aucun observé (pas de prédiction RT) : la preuve doit dire que le
        // verdict repose sur l'horaire théorique, pas sur une observation.
        let deps = vec![Departure {
            trip_id: "t".into(),
            headsign: "Test".into(),
            scheduled: Utc::now(),
            observed: None,
            cancelled: false,
        }];
        let v = evaluate(&deps, true, None, Thresholds::default());
        assert_eq!(v.status, ServiceStatus::Normal);
        assert!(has_evidence(&v, EvidenceKind::NoScheduledService));
        assert!(v.basis().contains("théorique"));
    }

    #[test]
    fn sans_rt_frais_et_aucun_service_reste_arrete() {
        // Le statique est fiable : pas de service prévu -> arrêt, même sans RT.
        let v = evaluate(&[], false, None, Thresholds::default());
        assert_eq!(v.status, ServiceStatus::Stopped);
    }
}
