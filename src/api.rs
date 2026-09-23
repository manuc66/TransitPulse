//! API HTTP (axum) + service du front statique.

use std::sync::{Arc, Mutex, RwLock};

use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use serde::{Deserialize, Serialize};
use tower_http::services::ServeDir;

use crate::domain::StopStatus;
use crate::gtfs::GtfsRepo;
use crate::realtime::RtState;
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
    pub rt: Arc<RwLock<RtState>>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/health", get(health))
        .route("/api/search", get(search))
        .route("/api/status", get(status))
        .route("/api/freshness", get(freshness))
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

async fn search(
    State(state): State<AppState>,
    Query(p): Query<SearchParams>,
) -> Json<Vec<crate::domain::Stop>> {
    if let Some(gtfs) = &state.gtfs
        && let Ok(stops) = gtfs.lock().expect("gtfs mutex").search_stops(&p.q, 10)
    {
        return Json(stops);
    }
    Json(state.repo.search(&p.q))
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
    // Résolution de l'arrêt cible.
    let stop_id = if let Some(id) = p.stop {
        id
    } else if let (Some(lat), Some(lon)) = (p.lat, p.lon) {
        let radius = p.radius.unwrap_or(500.0);
        let from_gtfs = state.gtfs.as_ref().and_then(|g| {
            g.lock()
                .expect("gtfs mutex")
                .nearest_stop(lat, lon, radius)
                .ok()
                .flatten()
                .map(|s| s.stop_id)
        });
        from_gtfs
            .or_else(|| {
                state
                    .repo
                    .nearest(lat, lon, radius)
                    .map(|s| s.stop_id.clone())
            })
            .ok_or_else(|| ApiError::NotFound("Aucun arrêt à proximité".into()))?
    } else {
        return Err(ApiError::BadRequest(
            "Fournir `stop` ou `lat` + `lon`".into(),
        ));
    };

    // Statut réel si le GTFS est chargé. Les horaires GTFS sont en heure de
    // Bruxelles : on ne doit PAS utiliser l'heure locale système (souvent UTC).
    if let Some(svc) = &state.status {
        let now = StatusService::now_brussels();
        let result = svc.lock().expect("status mutex").stop_status(&stop_id, now);
        return match result {
            Ok(Some(st)) => Ok(Json(st)),
            Ok(None) => Err(ApiError::NotFound(format!("Arrêt inconnu : {stop_id}"))),
            Err(e) => Err(ApiError::Internal(e.to_string())),
        };
    }

    // Repli sur le jeu fictif.
    state
        .repo
        .status_for_stop(&stop_id)
        .map(Json)
        .ok_or_else(|| ApiError::NotFound(format!("Arrêt inconnu : {stop_id}")))
}

#[derive(Serialize)]
struct Freshness {
    last_feed_ts: Option<i64>,
    feed_age_secs: Option<i64>,
    stale: bool,
    observations_last: usize,
    alerts_last: usize,
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
