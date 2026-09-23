use std::path::Path;
use std::time::Instant;
use transitpulse::gtfs;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url = gtfs::TEC_GTFS_URL;
    let cache = Path::new("data/raw/gtfs/tec-gtfs.zip");
    let db = Path::new("data/tec.sqlite");
    let client = reqwest::Client::builder()
        .user_agent("TransitPulse/0.1")
        .build()?;
    let t = Instant::now();
    let stats = gtfs::fetch_and_load(&client, url, cache, db).await?;
    println!("ETL en {:?}: {:?}", t.elapsed(), stats);

    let repo = gtfs::GtfsRepo::open(db)?;
    for q in ["Opéra", "Guillemins", "République"] {
        let stops = repo.search_stops(q, 3)?;
        println!("\n== recherche '{q}' -> {} arrêt(s)", stops.len());
        for s in &stops {
            let lines = repo.lines_for_stop(&s.stop_id)?;
            let names: Vec<_> = lines.iter().map(|l| l.short_name.clone()).collect();
            println!("  {} [{}] lignes: {:?}", s.name, s.stop_id, names);
        }
    }
    let ops = repo.search_stops("Guillemins", 3)?;
    if let Some(s) = ops.iter().find(|s| s.stop_id.starts_with("gs:tec:")) {
        let services = repo.active_services("20260923")?;
        println!("\n== services actifs le 20260923: {}", services.len());
        let deps = repo.departures_at_stop(&s.stop_id, &services, 0, 24 * 3600, 5)?;
        println!("\n== 5 prochains passages à {}", s.name);
        for d in deps {
            let h = d.departure_secs / 3600;
            let m = (d.departure_secs % 3600) / 60;
            println!("  {:02}:{:02} ligne {} -> {}", h, m, d.route_id, d.headsign);
        }
    }
    Ok(())
}
