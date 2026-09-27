use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;
use transitpulse::api::{AppState, router};
use transitpulse::archive::Archive;
use transitpulse::gtfs::{self, GtfsRepo};
use transitpulse::network::NetworkService;
use transitpulse::realtime::{self, RawArchive, RtConfig, RtState};
use transitpulse::state::Repo;
use transitpulse::status::StatusService;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let data_dir = std::env::var("TRANSITPULSE_DATA_DIR").unwrap_or_else(|_| "data".into());
    let db_path = Path::new(&data_dir).join("transitpulse.sqlite");
    let archive = Arc::new(Mutex::new(Archive::open(&db_path)?));
    tracing::info!(path = %db_path.display(), "archive temps réel ouverte");

    let raw_dir = Path::new(&data_dir).join("raw").join("rt");
    let retention: u64 = std::env::var("TRANSITPULSE_RAW_RETENTION_DAYS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(realtime::DEFAULT_RAW_RETENTION_DAYS);
    let raw = RawArchive::new(&raw_dir).with_retention(retention);
    tracing::info!(path = %raw_dir.display(), retention_days = retention, "archive brute configurée");

    let rt_state = Arc::new(RwLock::new(RtState::default()));

    let poll_secs: u64 = std::env::var("TRANSITPULSE_POLL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30);
    let cfg = RtConfig {
        poll_interval: Duration::from_secs(poll_secs),
        ..RtConfig::default()
    };

    let client = reqwest::Client::builder()
        .user_agent("TransitPulse/0.1 (+https://github.com/; open data CC BY 4.0)")
        .build()?;

    // ETL statique : télécharge si besoin, charge SQLite, puis ignore les échecs
    // (l'app retombe alors sur le jeu fictif plutôt que de refuser de démarrer).
    // Un ZIP en cache déjà décrit par `tec.sqlite` fait sauter le rechargement.
    let gtfs_db = Path::new(&data_dir).join("tec.sqlite");
    let cache = gtfs::gtfs_cache_path(Path::new(&data_dir));
    let gtfs_url =
        std::env::var("TRANSITPULSE_GTFS_URL").unwrap_or_else(|_| gtfs::TEC_GTFS_URL.to_string());
    let gtfs_max_age: u64 = std::env::var("TRANSITPULSE_GTFS_MAX_AGE_DAYS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(gtfs::DEFAULT_GTFS_MAX_AGE_DAYS);
    let (gtfs_repo, status_svc, network_svc) =
        match gtfs::fetch_and_load(&client, &gtfs_url, &cache, &gtfs_db, gtfs_max_age).await {
            Ok(load) => {
                tracing::info!(?load, "GTFS statique disponible");
                let repo = GtfsRepo::open(&gtfs_db)?;
                let svc = StatusService::open(&gtfs_db, &db_path)?;
                let net = NetworkService::open(&gtfs_db, &db_path)?;
                (
                    Some(Arc::new(Mutex::new(repo))),
                    Some(Arc::new(Mutex::new(svc))),
                    Some(Arc::new(Mutex::new(net))),
                )
            }
            Err(e) => {
                tracing::error!(error = %e, "ETL GTFS échoué — repli sur le jeu fictif");
                (None, None, None)
            }
        };

    let (events_tx, _events_rx) = tokio::sync::broadcast::channel(64);

    // Reconstruction de la vue réseau : une fois au démarrage, puis à chaque
    // cycle RT. Seul le premier passage scanne le GTFS statique (~5 s) ; les
    // suivants ne recalculent que les annulations, en quelques ms. Le rebuild
    // reste hors du chemin de requête HTTP (`spawn_blocking`), mais il tient le
    // `Mutex` du service : le faire au démarrage évite de bloquer `/api/network`
    // toutes les 30 s une fois l'application en service.
    if let Some(net) = &network_svc {
        let net = net.clone();
        let mut rx = events_tx.subscribe();
        tokio::spawn(async move {
            let net_initial = net.clone();
            let _ = tokio::task::spawn_blocking(move || {
                let started = std::time::Instant::now();
                match net_initial.lock().expect("network mutex").rebuild_stats() {
                    Ok(n) => tracing::info!(
                        communes = n,
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        "vue réseau initiale construite"
                    ),
                    Err(e) => tracing::warn!(error = %e, "rebuild réseau initial échoué"),
                }
            })
            .await;

            while rx.recv().await.is_ok() {
                let net = net.clone();
                let _ = tokio::task::spawn_blocking(move || {
                    let started = std::time::Instant::now();
                    match net.lock().expect("network mutex").rebuild_stats() {
                        Ok(n) => tracing::debug!(
                            communes = n,
                            elapsed_ms = started.elapsed().as_millis() as u64,
                            "vue réseau reconstruite"
                        ),
                        Err(e) => tracing::warn!(error = %e, "rebuild réseau échoué"),
                    }
                })
                .await;
            }
        });
    }

    let http_client = client.clone();
    tokio::spawn(realtime::run(
        client,
        archive.clone(),
        raw,
        rt_state.clone(),
        events_tx.clone(),
        cfg,
    ));

    let app = router(AppState {
        repo: Arc::new(Repo::mock()),
        gtfs: gtfs_repo,
        status: status_svc,
        network: network_svc,
        rt: rt_state,
        events: events_tx,
        http: http_client,
    });

    let addr = std::env::var("TRANSITPULSE_ADDR").unwrap_or_else(|_| "0.0.0.0:3000".into());
    let listener = TcpListener::bind(&addr).await?;
    tracing::info!("TransitPulse écoute sur http://{addr}");

    axum::serve(listener, app).await?;
    Ok(())
}
