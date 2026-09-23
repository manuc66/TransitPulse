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

/// Un résultat de géocodage : coordonnées + libellé lisible.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Geocoded {
    pub label: String,
    pub lat: f64,
    pub lon: f64,
}

/// Géocode une rue/adresse via Photon (géocodeur OpenStreetMap), avec repli
/// sur Nominatim. Renvoie le meilleur résultat, biaisé vers la Belgique.
pub async fn geocode(client: &reqwest::Client, query: &str) -> anyhow::Result<Option<Geocoded>> {
    let q = query.trim();
    if q.is_empty() {
        return Ok(None);
    }
    match photon(client, q).await {
        Ok(Some(g)) => Ok(Some(g)),
        _ => nominatim(client, q).await,
    }
}

async fn photon(client: &reqwest::Client, q: &str) -> anyhow::Result<Option<Geocoded>> {
    let resp: serde_json::Value = client
        .get("https://photon.komoot.io/api/")
        .query(&[("q", q), ("limit", "5"), ("lang", "fr")])
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let Some(features) = resp.get("features").and_then(|f| f.as_array()) else {
        return Ok(None);
    };
    // Priorise la Belgique.
    let mut ordered: Vec<&serde_json::Value> = features.iter().collect();
    ordered.sort_by_key(|f| {
        let cc = f
            .pointer("/properties/countrycode")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if cc == "BE" { 0 } else { 1 }
    });
    let Some(feature) = ordered.first() else {
        return Ok(None);
    };
    let coords = feature
        .pointer("/geometry/coordinates")
        .and_then(|c| c.as_array());
    let (Some(lon), Some(lat)) = (
        coords.and_then(|c| c.first()).and_then(|v| v.as_f64()),
        coords.and_then(|c| c.get(1)).and_then(|v| v.as_f64()),
    ) else {
        return Ok(None);
    };
    Ok(Some(Geocoded {
        label: photon_label(feature),
        lat,
        lon,
    }))
}

fn photon_label(feature: &serde_json::Value) -> String {
    let p = feature.pointer("/properties");
    let get = |k: &str| {
        p.and_then(|p| p.get(k))
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
    };
    let mut parts: Vec<String> = Vec::new();
    if let (Some(street), Some(num)) = (get("street"), get("housenumber")) {
        parts.push(format!("{street} {num}"));
    } else if let Some(name) = get("name") {
        parts.push(name.to_string());
    }
    for k in ["district", "city", "postcode"] {
        if let Some(v) = get(k)
            && !parts.iter().any(|x| x == v)
        {
            parts.push(v.to_string());
        }
    }
    if parts.is_empty() {
        "Adresse".to_string()
    } else {
        parts.join(", ")
    }
}

async fn nominatim(client: &reqwest::Client, q: &str) -> anyhow::Result<Option<Geocoded>> {
    let resp: serde_json::Value = client
        .get("https://nominatim.openstreetmap.org/search")
        .query(&[
            ("q", q),
            ("format", "json"),
            ("limit", "1"),
            ("countrycodes", "be"),
        ])
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let Some(first) = resp.as_array().and_then(|a| a.first()) else {
        return Ok(None);
    };
    let (Some(lat), Some(lon)) = (
        first
            .get("lat")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse().ok()),
        first
            .get("lon")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse().ok()),
    ) else {
        return Ok(None);
    };
    Ok(Some(Geocoded {
        label: first
            .get("display_name")
            .and_then(|v| v.as_str())
            .unwrap_or("Adresse")
            .to_string(),
        lat,
        lon,
    }))
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
