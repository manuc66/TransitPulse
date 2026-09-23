//! Géolocalisation : distance entre arrêts et (plus tard) géocodage rue/adresse.

/// Distance en mètres entre deux points (formule de haversine).
pub fn haversine_m(a_lat: f64, a_lon: f64, b_lat: f64, b_lon: f64) -> f64 {
    const EARTH_RADIUS_M: f64 = 6_371_000.0;
    let dlat = (b_lat - a_lat).to_radians();
    let dlon = (b_lon - a_lon).to_radians();
    let lat1 = a_lat.to_radians();
    let lat2 = b_lat.to_radians();
    let h = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
    2.0 * EARTH_RADIUS_M * h.sqrt().asin()
}

/// Géocode une rue/adresse en (lat, lon) via Nominatim.
///
/// Étape 5 du plan : non implémenté pour l'instant.
pub async fn geocode(_query: &str) -> anyhow::Result<(f64, f64)> {
    anyhow::bail!("geo::geocode non implémenté — étape 5 du plan")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distance_nulle_pour_meme_point() {
        assert!(haversine_m(50.6431, 5.5734, 50.6431, 5.5734) < 0.001);
    }

    #[test]
    fn distance_plausible_entre_liege_et_bruxelles() {
        // Liège <-> Bruxelles : ~90 km à vol d'oiseau.
        let d = haversine_m(50.6431, 5.5734, 50.8503, 4.3517);
        assert!(d > 80_000.0 && d < 100_000.0, "distance = {d}");
    }
}
