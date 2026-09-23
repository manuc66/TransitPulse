//! API HTTP (axum) + service du front statique.

use std::sync::{Arc, Mutex, RwLock};

use axum::{
    Json, Router,
    extract::{
        Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tower_http::services::ServeDir;

use crate::domain::StopStatus;
use crate::gtfs::GtfsRepo;
use crate::network::{CommuneStatus, NetworkService};
use crate::realtime::{LiveEvent, RtState};
use crate::state::Repo;
use crate::status::StatusService;

#[derive(Clone)]
pub struct AppState {
    /// Dépôt fictif (repli si le GTFS réel n'est pas chargé).
    pub repo: Arc<Repo>,
    /// Dépôt statique réel (None tant que l'ETL n'a pas tourné).
    /// `Mutex` car `rusqlite::Connection` n'est pas `Sync`.
    pub gtfs: Option<Arc<Mutex<GtfsRepo>>>,
    /// Service de statut réel.
    pub status: Option<Arc<Mutex<StatusService>>>,
    /// Vue systémique du réseau (None tant que l'ETL n'a pas tourné).
    pub network: Option<Arc<Mutex<NetworkService>>>,
    pub rt: Arc<RwLock<RtState>>,
    /// Client HTTP partagé (géocodage).
    pub http: reqwest::Client,
    /// Flux d'événements RT pour les abonnés WebSocket.
    pub events: broadcast::Sender<LiveEvent>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/health", get(health))
        .route("/api/search", get(search))
        .route("/api/status", get(status))
        .route("/api/nearby", get(nearby))
        .route("/api/network", get(network))
        .route("/api/freshness", get(freshness))
        .route("/api/live", get(live))
        .fallback_service(ServeDir::new("web"))
        .with_state(state)
}

async fn health(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "ok",
        "service": "transitpulse",
        "gtfs_loaded": state.gtfs.is_some(),
    }))
}

#[derive(Deserialize)]
struct SearchParams {
    q: String,
}

#[derive(Serialize)]
struct SearchResult {
    /// Arrêts correspondant au nom recherché.
    stops: Vec<crate::gtfs::NearbyStop>,
    /// Si la requête a été comprise comme une adresse : point géocodé.
    #[serde(skip_serializing_if = "Option::is_none")]
    origin: Option<crate::geo::Geocoded>,
    /// Message éventuel (adresse introuvable, hors couverture…).
    #[serde(skip_serializing_if = "Option::is_none")]
    note: Option<String>,
}

/// Heuristique : la requête ressemble-t-elle à une adresse (numéro de rue) ?
fn looks_like_address(q: &str) -> bool {
    q.chars().any(|c| c.is_ascii_digit())
}

async fn search(
    State(state): State<AppState>,
    Query(p): Query<SearchParams>,
) -> Json<SearchResult> {
    let Some(gtfs) = &state.gtfs else {
        let stops = state
            .repo
            .search(&p.q)
            .into_iter()
            .map(Into::into)
            .collect();
        return Json(SearchResult {
            stops,
            origin: None,
            note: None,
        });
    };

    // 1) Recherche par nom d'arrêt.
    let by_name = gtfs
        .lock()
        .expect("gtfs mutex")
        .search_stops(&p.q, 10)
        .unwrap_or_default();
    if !by_name.is_empty() && !looks_like_address(&p.q) {
        return Json(SearchResult {
            stops: nearby_from_stops(gtfs, &by_name),
            origin: None,
            note: None,
        });
    }

    // 2) Aucun arrêt (ou adresse) : géocodage puis arrêts les plus proches.
    match crate::geo::geocode(&state.http, &p.q).await {
        Ok(Some(g)) => {
            let radius = 1200.0;
            let stops = gtfs
                .lock()
                .expect("gtfs mutex")
                .nearby_stops(g.lat, g.lon, radius)
                .unwrap_or_default();
            let note = if stops.is_empty() {
                Some(format!(
                    "Aucun arrêt dans un rayon de {radius:.0} m autour de « {} »",
                    g.label
                ))
            } else {
                None
            };
            Json(SearchResult {
                stops,
                origin: Some(g),
                note,
            })
        }
        _ => {
            // Repli : renvoie ce que la recherche par nom a trouvé (souvent vide).
            Json(SearchResult {
                stops: nearby_from_stops(gtfs, &by_name),
                origin: None,
                note: if by_name.is_empty() {
                    Some("Adresse introuvable".to_string())
                } else {
                    None
                },
            })
        }
    }
}

/// Convertit des `Stop` (sans distance) en `NearbyStop` (distance inconnue = 0,
/// desservi à vérifier côté requête).
fn nearby_from_stops(
    gtfs: &Arc<Mutex<GtfsRepo>>,
    stops: &[crate::domain::Stop],
) -> Vec<crate::gtfs::NearbyStop> {
    let guard = gtfs.lock().expect("gtfs mutex");
    stops
        .iter()
        .map(|s| {
            let served = guard.stop_is_served(&s.stop_id).unwrap_or(false);
            crate::gtfs::NearbyStop {
                stop: s.clone(),
                metres: 0.0,
                served,
            }
        })
        .collect()
}

#[derive(Deserialize)]
struct StatusParams {
    stop: Option<String>,
    lat: Option<f64>,
    lon: Option<f64>,
    radius: Option<f64>,
}

async fn status(
    State(state): State<AppState>,
    Query(p): Query<StatusParams>,
) -> Result<Json<StopStatus>, ApiError> {
    let stop_id = resolve_stop_id(&state, p.stop, p.lat, p.lon, p.radius)?;
    compute_status(&state, &stop_id).map(Json)
}

#[derive(Deserialize)]
struct NearbyParams {
    lat: f64,
    lon: f64,
    radius: Option<f64>,
}

/// Arrêts à proximité, triés par distance : alimente la géolocalisation navigateur.
async fn nearby(
    State(state): State<AppState>,
    Query(p): Query<NearbyParams>,
) -> Result<Json<Vec<crate::gtfs::NearbyStop>>, ApiError> {
    let radius = p.radius.unwrap_or(800.0);
    let Some(gtfs) = &state.gtfs else {
        return Err(ApiError::Internal("GTFS non chargé".into()));
    };
    let stops = gtfs
        .lock()
        .expect("gtfs mutex")
        .nearby_stops(p.lat, p.lon, radius)
        .map_err(|e| ApiError::Internal(e.to_string()))?;
    Ok(Json(stops))
}

/// Résout l'arrêt cible depuis un `stop_id` ou des coordonnées.
fn resolve_stop_id(
    state: &AppState,
    stop: Option<String>,
    lat: Option<f64>,
    lon: Option<f64>,
    radius: Option<f64>,
) -> Result<String, ApiError> {
    if let Some(id) = stop {
        return Ok(id);
    }
    if let (Some(lat), Some(lon)) = (lat, lon) {
        let radius = radius.unwrap_or(500.0);
        let from_gtfs = state.gtfs.as_ref().and_then(|g| {
            g.lock()
                .expect("gtfs mutex")
                .nearest_stop(lat, lon, radius)
                .ok()
                .flatten()
                .map(|s| s.stop_id)
        });
        return from_gtfs
            .or_else(|| {
                state
                    .repo
                    .nearest(lat, lon, radius)
                    .map(|s| s.stop_id.clone())
            })
            .ok_or_else(|| ApiError::NotFound("Aucun arrêt à proximité".into()));
    }
    Err(ApiError::BadRequest(
        "Fournir `stop` ou `lat` + `lon`".into(),
    ))
}

/// Calcule le statut d'un arrêt (réel si GTFS chargé, sinon jeu fictif).
fn compute_status(state: &AppState, stop_id: &str) -> Result<StopStatus, ApiError> {
    // Les horaires GTFS sont en heure de Bruxelles : on ne doit PAS utiliser
    // l'heure locale système (souvent UTC).
    if let Some(svc) = &state.status {
        let now = StatusService::now_brussels();
        let result = svc.lock().expect("status mutex").stop_status(stop_id, now);
        return match result {
            Ok(Some(st)) => Ok(st),
            Ok(None) => Err(ApiError::NotFound(format!("Arrêt inconnu : {stop_id}"))),
            Err(e) => Err(ApiError::Internal(e.to_string())),
        };
    }
    state
        .repo
        .status_for_stop(stop_id)
        .ok_or_else(|| ApiError::NotFound(format!("Arrêt inconnu : {stop_id}")))
}

/// WebSocket `/api/live?stop=<id>` : pousse le statut dès chaque cycle temps réel.
async fn live(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Query(p): Query<StatusParams>,
) -> Result<Response, ApiError> {
    let stop_id = resolve_stop_id(&state, p.stop, p.lat, p.lon, p.radius)?;
    Ok(ws.on_upgrade(move |socket| live_loop(socket, state, stop_id)))
}

async fn live_loop(mut socket: WebSocket, state: AppState, stop_id: String) {
    let mut events = state.events.subscribe();
    let mut current = stop_id;

    // Envoi initial immédiat, avant le premier cycle.
    if push_status(&mut socket, &state, &current).await.is_err() {
        return;
    }

    loop {
        tokio::select! {
            evt = events.recv() => match evt {
                // `Lagged` = on a raté des cycles : on renvoie quand même l'état
                // courant, qui est toujours le plus récent.
                Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {
                    if push_status(&mut socket, &state, &current).await.is_err() {
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Closed) => break,
            },
            incoming = socket.recv() => match incoming {
                // Le client peut changer d'arrêt en renvoyant un `stop_id` brut.
                Some(Ok(Message::Text(txt))) => {
                    let new_id = txt.trim().to_string();
                    if !new_id.is_empty() && new_id != current {
                        current = new_id;
                        if push_status(&mut socket, &state, &current).await.is_err() {
                            break;
                        }
                    }
                }
                Some(Ok(Message::Close(_))) | None => break,
                _ => {}
            },
        }
    }
}

async fn push_status(socket: &mut WebSocket, state: &AppState, stop_id: &str) -> Result<(), ()> {
    let msg = match compute_status(state, stop_id) {
        Ok(status) => serde_json::to_string(&LiveMessage::Status {
            stop_id: stop_id.to_string(),
            status: Box::new(status),
        }),
        Err(ApiError::NotFound(m)) => Ok(serde_json::to_string(&LiveMessage::Error {
            stop_id: stop_id.to_string(),
            message: m,
        })
        .unwrap_or_default()),
        Err(_) => Ok(serde_json::to_string(&LiveMessage::Error {
            stop_id: stop_id.to_string(),
            message: "Erreur interne".into(),
        })
        .unwrap_or_default()),
    };
    match msg {
        Ok(text) => socket
            .send(Message::Text(text.into()))
            .await
            .map_err(|_| ()),
        Err(_) => Err(()),
    }
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum LiveMessage {
    Status {
        stop_id: String,
        status: Box<StopStatus>,
    },
    Error {
        stop_id: String,
        message: String,
    },
}

#[derive(Deserialize)]
struct NetworkParams {
    commune: Option<String>,
    /// Si fourni (ex. `gr:tec:L0032-20034`), renvoie le ratio de la ligne.
    line: Option<String>,
}

#[derive(Serialize)]
struct NetworkView {
    /// Commune la plus préoccupante du réseau.
    worst: Option<CommuneStatus>,
    /// Nombre de communes en état dégradé ou critique (signal d'événement).
    degraded_communes: usize,
    critical_communes: usize,
    communes: Vec<CommuneStatus>,
    /// Zones/dépôts TEC, triés par part d'annulation.
    zones: Vec<crate::network::ZoneStatus>,
    /// Ratio ciblé d'une ligne (si `line=` demandé).
    #[serde(skip_serializing_if = "Option::is_none")]
    line: Option<crate::network::LineStatus>,
}

/// Vue systémique du réseau : annulations par commune/zone, détection d'événement.
async fn network(
    State(state): State<AppState>,
    Query(p): Query<NetworkParams>,
) -> Result<Json<NetworkView>, ApiError> {
    let Some(net) = &state.network else {
        return Err(ApiError::Internal("GTFS non chargé".into()));
    };
    let net = net.lock().expect("network mutex");
    let communes = net
        .communes(p.commune.as_deref())
        .map_err(|e| ApiError::Internal(e.to_string()))?;
    let zones = net.zones().map_err(|e| ApiError::Internal(e.to_string()))?;
    let line = match &p.line {
        Some(rid) => net
            .line_ratio(rid)
            .map_err(|e| ApiError::Internal(e.to_string()))?,
        None => None,
    };
    let degraded = communes
        .iter()
        .filter(|c| c.status == crate::network::NetworkStatus::Degraded)
        .count();
    let critical = communes
        .iter()
        .filter(|c| c.status == crate::network::NetworkStatus::Critical)
        .count();
    // Ne remonte que les signalées, top 20, pour éviter un payload énorme.
    let mut shown: Vec<_> = communes
        .iter()
        .filter(|c| c.status != crate::network::NetworkStatus::Normal)
        .cloned()
        .collect();
    shown.truncate(20);
    let worst = shown.first().cloned();
    Ok(Json(NetworkView {
        worst,
        degraded_communes: degraded,
        critical_communes: critical,
        communes: shown,
        zones,
        line,
    }))
}

#[derive(Serialize)]
struct Freshness {
    last_feed_ts: Option<i64>,
    feed_age_secs: Option<i64>,
    stale: bool,
    observations_last: usize,
    alerts_last: usize,
    alert_trips_last: usize,
    last_error: Option<String>,
}

async fn freshness(State(state): State<AppState>) -> Json<Freshness> {
    let now = chrono::Utc::now().timestamp();
    let rt = state.rt.read().expect("état RT empoisonné");
    Json(Freshness {
        last_feed_ts: rt.last_feed_ts,
        feed_age_secs: rt.feed_age_secs(now),
        stale: rt.is_stale(now, state.repo.thresholds.stale_feed_secs as i64),
        observations_last: rt.observations_last,
        alerts_last: rt.alerts_last,
        alert_trips_last: rt.alert_trips_last,
        last_error: rt.last_error.clone(),
    })
}

pub enum ApiError {
    BadRequest(String),
    NotFound(String),
    Internal(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (code, msg) = match self {
            ApiError::BadRequest(m) => (StatusCode::BAD_REQUEST, m),
            ApiError::NotFound(m) => (StatusCode::NOT_FOUND, m),
            ApiError::Internal(m) => (StatusCode::INTERNAL_SERVER_ERROR, m),
        };
        (code, Json(serde_json::json!({ "error": msg }))).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::looks_like_address;

    #[test]
    fn detection_adresse() {
        assert!(looks_like_address("Rue de la Station 12"));
        assert!(looks_like_address("Place Centrale 1"));
        assert!(!looks_like_address("Guillemins"));
        assert!(!looks_like_address("Opéra"));
    }
}
