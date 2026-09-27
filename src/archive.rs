//! Archive append-only du temps réel (SQLite).
//!
//! Les feeds GTFS-RT sont écrasés toutes les 30 s : ce qui n'est pas capturé
//! est perdu à jamais. Cette archive est donc la matière première irremplaçable
//! des futures statistiques de régularité (rue, ligne, arrêt, jour, tranche horaire).
//!
//! ## Pourquoi les alertes ne sont pas versionnées par `feed_ts`
//!
//! Un premier modèle stockait une copie *complète* de chaque alerte à chaque
//! cycle, clé `(feed_ts, alert_id)`. Sur le feed TEC cela faisait 677 alertes
//! reproduites 2 880 fois par jour, soit ~550 Ko par cycle et ~1,6 Go sur la
//! rétention de 90 jours — pour du texte que personne ne relit.
//!
//! Mesure faite sur le feed réel : le texte d'une alerte (`header_fr`,
//! `description_fr`, `effect`, `severity`, `cause`, fenêtre d'activité) ne change
//! **jamais** ; seul le périmètre (`route_ids`, `stop_ids`) évolue, quelques fois
//! par jour, quand TEC affine la liste des courses touchées. Le jeu de courses
//! ciblées ne bouge pas non plus.
//!
//! Le modèle retient donc ces deux regularités :
//!
//! - le contenu vit dans `rt_alerts`, une ligne par `alert_id`, avec
//!   `first_seen`/`last_seen` qui disent dans quel cycle il est actif. Un
//!   rechargement ne réécrit que la ligne concernée ;
//! - chaque changement de périmètre est journalisé dans `rt_alert_scopes`, qui
//!   reste minuscule puisqu'elle ne reçoit qu'une écriture par *changement*, pas
//!   par cycle.
//!
//! « Est-ce que cette alerte est dans le feed courant ? » se lit alors
//! `last_seen = (SELECT MAX(last_seen) FROM rt_alerts)`, contre
//! `feed_ts = (SELECT MAX(feed_ts) FROM rt_alerts)` auparavant : même sémantique,
//! sans la redondance.
//!
//! Ce sélectre repose sur une propriété du cycle : chaque poll ré-annonce
//! toutes les alertes encore actives, donc `MAX(last_seen)` avance à chaque
//! poll qui rapporte au moins une alerte. Une alerte disparue du feed cesse donc
//! d'être « courante » dès le poll suivant. Un poll qui ne rapporte *aucune*
//! alerte laisse le sélectre sur le cycle précédent — comportement identique à
//! celui de l'ancien `MAX(feed_ts)`, et sans cas dégradant : l'UI affiche alors
//! un jeu d'alertes légèrement périmé plutôt qu'un état vide.
//!
//! `rt_observations` garde son modèle append-only : une observation de retard a
//! une valeur propre, on ne peut pas la dédupliquer.

use std::path::Path;

use anyhow::Result;
use rusqlite::{Connection, params};

/// Version du schéma de l'archive. Passée à 2 lors de la déduplication des
/// alertes, à 3 lors du retrait des index inutilisés de `rt_observations`.
/// Sert de déclencheur aux étapes de `migrate`.
const SCHEMA_VERSION: i64 = 3;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS rt_observations (
    feed_ts               INTEGER NOT NULL,
    captured_at           INTEGER NOT NULL,
    trip_id               TEXT    NOT NULL,
    route_id              TEXT,
    stop_id               TEXT    NOT NULL,
    stop_sequence         INTEGER NOT NULL,
    scheduled_ms          INTEGER,
    predicted_ms          INTEGER,
    delay_s               INTEGER,
    schedule_relationship INTEGER NOT NULL,
    PRIMARY KEY (feed_ts, trip_id, stop_sequence)
);
-- Aucun index secondaire sur rt_observations, et c'est délibéré.
--
-- La seule requête qui lit cette table (`StatusService::latest_rt_for_stop`)
-- s'appuie sur `feed_ts = (SELECT MAX(feed_ts) ...)`, que la clé primaire
-- couvre déjà, puis joint `gtfs_trips`/`gtfs_stop_times` par `trip_id`.
-- `EXPLAIN QUERY PLAN` confirme qu'elle ne touche aucun autre index :
--
--   SEARCH o USING INDEX sqlite_autoindex_rt_observations_1 (feed_ts=?)
--
-- Les trois index qui existaient (`stop_id, feed_ts`, `route_id, feed_ts`,
-- `trip_id`) coûtaient 7,4 Mo et étaient réécrits à chaque insertion, pour
-- 2,1 -> 2,7 ms sur la requête : de l'amplification d'écriture pure.
--
-- Ils restent utiles le jour où les statistiques annoncées dans l'en-tête du
-- module seront écrites (retard moyen par arrêt, par heure). À recréer alors :
--
--   CREATE INDEX idx_obs_stop_ts  ON rt_observations(stop_id, feed_ts);
--   CREATE INDEX idx_obs_route_ts ON rt_observations(route_id, feed_ts);
--   CREATE INDEX idx_obs_trip     ON rt_observations(trip_id);
--
-- Measurement refaite après suppression : 2,7 ms, plan identique.

-- Une ligne par alerte. Le contenu est écrasé à chaque sighting ; `first_seen`
-- est conservé car c'est le premier passage observé, pas le dernier.
CREATE TABLE IF NOT EXISTS rt_alerts (
    alert_id       TEXT    PRIMARY KEY,
    agency_id      TEXT,
    effect         INTEGER,
    severity       INTEGER,
    cause          INTEGER,
    header_fr      TEXT,
    description_fr TEXT,
    active_from    INTEGER,
    active_to      INTEGER,
    route_ids      TEXT    NOT NULL,
    stop_ids       TEXT    NOT NULL,
    first_seen     INTEGER NOT NULL,
    last_seen      INTEGER NOT NULL
);
-- Sert au sélecteur « alerte présente dans le feed courant ».
CREATE INDEX IF NOT EXISTS idx_alerts_last_seen ON rt_alerts(last_seen);

-- Historique du périmètre : une écriture par changement, pas par cycle.
CREATE TABLE IF NOT EXISTS rt_alert_scopes (
    alert_id  TEXT    NOT NULL,
    since_ts  INTEGER NOT NULL,
    route_ids TEXT    NOT NULL,
    stop_ids  TEXT    NOT NULL,
    PRIMARY KEY (alert_id, since_ts)
);

CREATE TABLE IF NOT EXISTS rt_alert_trips (
    alert_id   TEXT    NOT NULL,
    trip_id    TEXT    NOT NULL,
    start_date TEXT,
    first_seen INTEGER NOT NULL,
    last_seen  INTEGER NOT NULL,
    PRIMARY KEY (alert_id, trip_id)
);
CREATE INDEX IF NOT EXISTS idx_alert_trips_trip ON rt_alert_trips(trip_id, last_seen);

-- Vue réseau matérialisée (commune -> courses prévues/annulées), reconstruite
-- une fois par cycle RT pour rester à coût constant côté requêtes HTTP.
CREATE TABLE IF NOT EXISTS network_stats (
    commune          TEXT PRIMARY KEY,
    trips_scheduled  INTEGER NOT NULL,
    trips_cancelled  INTEGER NOT NULL
);

-- Même vue, agrégée par zone/dépôt TEC (préfixe de ligne : L, H, C, N, B, X).
CREATE TABLE IF NOT EXISTS network_zones (
    zone             TEXT PRIMARY KEY,
    trips_scheduled  INTEGER NOT NULL,
    trips_cancelled  INTEGER NOT NULL
);
"#;

/// Migration 1 -> 2 : aplatit les alertes versionnées par `feed_ts`.
///
/// L'ancienne base contient N copies d'une même alerte. On en garde une, en
/// prenant le contenu de sa copie la plus récente et en retenant le premier et
/// le dernier `feed_ts` comme bornes de présence. Le périmètre devient
/// l'historique `rt_alert_scopes` : autant de lignes que de combinaisons
/// distinctes réellement observées, pas une par cycle.
const MIGRATE_1_TO_2: &str = r#"
CREATE TABLE rt_alerts_v2 (
    alert_id       TEXT    PRIMARY KEY,
    agency_id      TEXT,
    effect         INTEGER,
    severity       INTEGER,
    cause          INTEGER,
    header_fr      TEXT,
    description_fr TEXT,
    active_from    INTEGER,
    active_to      INTEGER,
    route_ids      TEXT    NOT NULL,
    stop_ids       TEXT    NOT NULL,
    first_seen     INTEGER NOT NULL,
    last_seen      INTEGER NOT NULL
);
INSERT INTO rt_alerts_v2
     (alert_id, agency_id, effect, severity, cause, header_fr, description_fr,
      active_from, active_to, route_ids, stop_ids, first_seen, last_seen)
SELECT a.alert_id,
       (SELECT x.agency_id      FROM rt_alerts x WHERE x.alert_id = a.alert_id ORDER BY x.feed_ts DESC LIMIT 1),
       (SELECT x.effect         FROM rt_alerts x WHERE x.alert_id = a.alert_id ORDER BY x.feed_ts DESC LIMIT 1),
       (SELECT x.severity       FROM rt_alerts x WHERE x.alert_id = a.alert_id ORDER BY x.feed_ts DESC LIMIT 1),
       (SELECT x.cause          FROM rt_alerts x WHERE x.alert_id = a.alert_id ORDER BY x.feed_ts DESC LIMIT 1),
       (SELECT x.header_fr      FROM rt_alerts x WHERE x.alert_id = a.alert_id ORDER BY x.feed_ts DESC LIMIT 1),
       (SELECT x.description_fr FROM rt_alerts x WHERE x.alert_id = a.alert_id ORDER BY x.feed_ts DESC LIMIT 1),
       (SELECT x.active_from    FROM rt_alerts x WHERE x.alert_id = a.alert_id ORDER BY x.feed_ts DESC LIMIT 1),
       (SELECT x.active_to      FROM rt_alerts x WHERE x.alert_id = a.alert_id ORDER BY x.feed_ts DESC LIMIT 1),
       (SELECT x.route_ids      FROM rt_alerts x WHERE x.alert_id = a.alert_id ORDER BY x.feed_ts DESC LIMIT 1),
       (SELECT x.stop_ids       FROM rt_alerts x WHERE x.alert_id = a.alert_id ORDER BY x.feed_ts DESC LIMIT 1),
       MIN(a.feed_ts), MAX(a.feed_ts)
  FROM rt_alerts a
 GROUP BY a.alert_id;

CREATE TABLE rt_alert_scopes_v2 (
    alert_id  TEXT    NOT NULL,
    since_ts  INTEGER NOT NULL,
    route_ids TEXT    NOT NULL,
    stop_ids  TEXT    NOT NULL,
    PRIMARY KEY (alert_id, since_ts)
);
-- `since_ts` = première sighting de CETTE combinaison de périmètre : plusieurs
-- combinaisons peuvent partager la même alert_id, chacune avec sa propre date.
INSERT INTO rt_alert_scopes_v2 (alert_id, since_ts, route_ids, stop_ids)
SELECT alert_id, MIN(feed_ts), route_ids, stop_ids
  FROM rt_alerts
 GROUP BY alert_id, route_ids, stop_ids;

CREATE TABLE rt_alert_trips_v2 (
    alert_id   TEXT    NOT NULL,
    trip_id    TEXT    NOT NULL,
    start_date TEXT,
    first_seen INTEGER NOT NULL,
    last_seen  INTEGER NOT NULL,
    PRIMARY KEY (alert_id, trip_id)
);
INSERT INTO rt_alert_trips_v2 (alert_id, trip_id, start_date, first_seen, last_seen)
SELECT at.alert_id,
       at.trip_id,
       (SELECT x.start_date FROM rt_alert_trips x
         WHERE x.alert_id = at.alert_id AND x.trip_id = at.trip_id
         ORDER BY x.feed_ts DESC LIMIT 1),
       MIN(at.feed_ts), MAX(at.feed_ts)
  FROM rt_alert_trips at
 GROUP BY at.alert_id, at.trip_id;

DROP TABLE rt_alerts;
DROP TABLE rt_alert_trips;
ALTER TABLE rt_alerts_v2   RENAME TO rt_alerts;
ALTER TABLE rt_alert_trips_v2 RENAME TO rt_alert_trips;
ALTER TABLE rt_alert_scopes_v2 RENAME TO rt_alert_scopes;
CREATE INDEX idx_alerts_last_seen  ON rt_alerts(last_seen);
CREATE INDEX idx_alert_trips_trip ON rt_alert_trips(trip_id, last_seen);
DROP INDEX IF EXISTS idx_alerts_route;
"#;

/// Migration 2 -> 3 : retire les index secondaires de `rt_observations`.
///
/// Aucun n'est utilisé par l'application — voir le commentaire du schéma pour
/// le `EXPLAIN QUERY PLAN` et les mesures. Les retirer suffit à stopper
/// l'amplification d'écriture ; le `VACUUM` qui suit rend les pages au système
/// de fichiers, puisque `DROP INDEX` ne fait que les pousser dans la freelist.
const MIGRATE_2_TO_3: &str = r#"
DROP INDEX IF EXISTS idx_obs_stop_ts;
DROP INDEX IF EXISTS idx_obs_route_ts;
DROP INDEX IF EXISTS idx_obs_trip;
"#;

/// Une observation de passage issue du feed `trip-update`.
///
/// `scheduled_ms` est reconstruit depuis `predicted_ms - delay_s * 1000`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Observation {
    pub feed_ts: i64,
    pub captured_at: i64,
    pub trip_id: String,
    pub route_id: Option<String>,
    pub stop_id: String,
    pub stop_sequence: i64,
    pub scheduled_ms: Option<i64>,
    pub predicted_ms: Option<i64>,
    pub delay_s: Option<i64>,
    pub schedule_relationship: i64,
}

/// Une alerte de service issue du feed `alert`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlertRecord {
    pub feed_ts: i64,
    pub captured_at: i64,
    pub alert_id: String,
    pub agency_id: Option<String>,
    pub effect: Option<i64>,
    pub severity: Option<i64>,
    pub cause: Option<i64>,
    pub route_ids: String,
    pub stop_ids: String,
    pub header_fr: Option<String>,
    pub description_fr: Option<String>,
    pub active_from: Option<i64>,
    pub active_to: Option<i64>,
}

/// Lien alerte ↔ course : TEC cible les annulations sur une course précise.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlertTrip {
    pub feed_ts: i64,
    pub alert_id: String,
    pub trip_id: String,
    pub start_date: Option<String>,
}

pub struct Archive {
    conn: Connection,
}

impl Archive {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        Self::init(Connection::open(path)?)
    }

    pub fn open_in_memory() -> Result<Self> {
        Self::init(Connection::open_in_memory()?)
    }

    fn init(conn: Connection) -> Result<Self> {
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        migrate(&conn)?;
        conn.execute_batch(SCHEMA)?;
        Ok(Self { conn })
    }

    /// Insère les observations. Idempotent : une même clé n'est écrite qu'une fois.
    pub fn insert_observations(&mut self, rows: &[Observation]) -> Result<usize> {
        let tx = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT OR REPLACE INTO rt_observations
                 (feed_ts, captured_at, trip_id, route_id, stop_id, stop_sequence,
                  scheduled_ms, predicted_ms, delay_s, schedule_relationship)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            )?;
            for r in rows {
                stmt.execute(params![
                    r.feed_ts,
                    r.captured_at,
                    r.trip_id,
                    r.route_id,
                    r.stop_id,
                    r.stop_sequence,
                    r.scheduled_ms,
                    r.predicted_ms,
                    r.delay_s,
                    r.schedule_relationship,
                ])?;
            }
        }
        tx.commit()?;
        Ok(rows.len())
    }

    /// Archive les alertes du cycle, sans dupliquer leur contenu.
    ///
    /// Un `alert_id` déjà connu est mis à jour en place ; `first_seen` n'est
    /// jamais réécrit, `last_seen` avance. Le périmètre (`route_ids`,
    /// `stop_ids`) est journalisé dans `rt_alert_scopes` **avant** la mise à
    /// jour, et seulement à deux moments : la première sighting, ou un
    /// changement. C'est ce qui évite d'écrire une ligne par cycle alors que le
    /// périmètre bouge rarement.
    ///
    /// `rt_alert_scopes` est ainsi la timeline complète du périmètre : le
    /// périmètre au cycle `T` se lit en prenant la ligne de `since_ts` la plus
    /// grande <= `T`.
    pub fn insert_alerts(&mut self, rows: &[AlertRecord]) -> Result<usize> {
        let tx = self.conn.transaction()?;
        {
            let mut scope = tx.prepare(
                "INSERT INTO rt_alert_scopes (alert_id, since_ts, route_ids, stop_ids)
                 SELECT ?1, ?2, ?3, ?4
                 WHERE NOT EXISTS (SELECT 1 FROM rt_alerts a WHERE a.alert_id = ?1)
                    OR EXISTS (SELECT 1 FROM rt_alerts a
                                WHERE a.alert_id = ?1
                                  AND (a.route_ids <> ?3 OR a.stop_ids <> ?4))",
            )?;
            let mut upsert = tx.prepare(
                "INSERT INTO rt_alerts
                 (alert_id, agency_id, effect, severity, cause, header_fr, description_fr,
                  active_from, active_to, route_ids, stop_ids, first_seen, last_seen)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?12)
                 ON CONFLICT(alert_id) DO UPDATE SET
                    agency_id      = excluded.agency_id,
                    effect         = excluded.effect,
                    severity       = excluded.severity,
                    cause          = excluded.cause,
                    header_fr      = excluded.header_fr,
                    description_fr = excluded.description_fr,
                    active_from    = excluded.active_from,
                    active_to      = excluded.active_to,
                    route_ids      = excluded.route_ids,
                    stop_ids       = excluded.stop_ids,
                    last_seen      = excluded.last_seen",
            )?;
            for r in rows {
                scope.execute(params![r.alert_id, r.feed_ts, r.route_ids, r.stop_ids])?;
                upsert.execute(params![
                    r.alert_id,
                    r.agency_id,
                    r.effect,
                    r.severity,
                    r.cause,
                    r.header_fr,
                    r.description_fr,
                    r.active_from,
                    r.active_to,
                    r.route_ids,
                    r.stop_ids,
                    r.feed_ts,
                ])?;
            }
        }
        tx.commit()?;
        Ok(rows.len())
    }

    /// Archive les liens alerte ↔ course, sans les rejouer à chaque cycle.
    ///
    /// Même logique que [`Archive::insert_alerts`] : `first_seen` est conservé,
    /// `last_seen` avance, et une ligne déjà présente n'est réécrite qu'en mise à
    /// jour. Le jeu de courses ciblées étant stable sur le feed TEC, la table ne
    /// cesse de grossir.
    pub fn insert_alert_trips(&mut self, rows: &[AlertTrip]) -> Result<usize> {
        let tx = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT INTO rt_alert_trips (alert_id, trip_id, start_date, first_seen, last_seen)
                 VALUES (?1, ?2, ?3, ?4, ?4)
                 ON CONFLICT(alert_id, trip_id) DO UPDATE SET
                    start_date = excluded.start_date,
                    last_seen  = excluded.last_seen",
            )?;
            for r in rows {
                stmt.execute(params![r.alert_id, r.trip_id, r.start_date, r.feed_ts])?;
            }
        }
        tx.commit()?;
        Ok(rows.len())
    }

    pub fn alert_trip_count(&self) -> Result<i64> {
        Ok(self
            .conn
            .query_row("SELECT COUNT(*) FROM rt_alert_trips", [], |r| r.get(0))?)
    }

    pub fn observation_count(&self) -> Result<i64> {
        Ok(self
            .conn
            .query_row("SELECT COUNT(*) FROM rt_observations", [], |r| r.get(0))?)
    }

    /// Nombre d'alertes distinctes connues, toutes périodes confondues.
    pub fn alert_count(&self) -> Result<i64> {
        Ok(self
            .conn
            .query_row("SELECT COUNT(*) FROM rt_alerts", [], |r| r.get(0))?)
    }

    /// Nombre d'alertes présentes dans le dernier feed archivé.
    ///
    /// C'est cette valeur qui correspond à l'ancien `COUNT(*)` : l'ancien
    /// schéma ne pouvait compter qu'un cycle à la fois puisque chaque ligne
    /// portait son `feed_ts`.
    pub fn alert_count_current(&self) -> Result<i64> {
        Ok(self.conn.query_row(
            "SELECT COUNT(*) FROM rt_alerts
             WHERE last_seen = (SELECT MAX(last_seen) FROM rt_alerts)",
            [],
            |r| r.get(0),
        )?)
    }

    /// Nombre de changements de périmètre journalisés.
    pub fn alert_scope_count(&self) -> Result<i64> {
        Ok(self
            .conn
            .query_row("SELECT COUNT(*) FROM rt_alert_scopes", [], |r| r.get(0))?)
    }

    /// Dernier `feed_ts` observé, toutes tables confondues.
    pub fn last_feed_ts(&self) -> Result<Option<i64>> {
        Ok(self.conn.query_row(
            "SELECT MAX(feed_ts) FROM (
                SELECT feed_ts FROM rt_observations
                UNION ALL SELECT last_seen FROM rt_alerts)",
            [],
            |r| r.get(0),
        )?)
    }
}

/// Applique les migrations de schéma selon `PRAGMA user_version`.
///
/// `user_version` vit dans l'en-tête du fichier SQLite : pas de table de suivi
/// à maintenir, et une base neuve démarre simplement à 0.
///
/// Seule exception : une base écrite par une version du code *antérieure* à ce
/// mécanisme a `user_version = 0` tout en ayant déjà les tables. On la
/// reconnaît à la présence de celles-ci et on la traite comme une v1.
fn migrate(conn: &Connection) -> Result<()> {
    let mut version: i64 = conn.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    if version >= SCHEMA_VERSION {
        return Ok(());
    }
    let tables: i64 = conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master
         WHERE type = 'table' AND name IN ('rt_observations', 'rt_alerts', 'rt_alert_trips')",
        [],
        |r| r.get(0),
    )?;
    if tables == 0 {
        // Base neuve : `SCHEMA` fait le travail, on se contente de versions.
        conn.pragma_update(None, "user_version", SCHEMA_VERSION)?;
        return Ok(());
    }
    if version == 0 {
        version = 1; // schéma d'avant le versionnement
    }

    tracing::info!(
        from = version,
        to = SCHEMA_VERSION,
        "migration de l'archive temps réel"
    );
    while version < SCHEMA_VERSION {
        let next = version + 1;
        let step = match version {
            1 => MIGRATE_1_TO_2,
            2 => MIGRATE_2_TO_3,
            other => anyhow::bail!("aucune migration d'archive depuis la version {other}"),
        };
        let tx = conn.unchecked_transaction()?;
        tx.execute_batch(step)?;
        tx.pragma_update(None, "user_version", next)?;
        tx.commit()?;
        version = next;
    }

    // Les étapes libèrent des pages mais, comme partout dans SQLite, ni un
    // `DROP TABLE` ni un `DROP INDEX` ne rend l'espace au système de fichiers :
    // tout reste dans la freelist. Un VACUUM est donc nécessaire pour que la
    // migration récupère réellement le disque. Il est faisable ici parce que
    // les migrations sont rares, hors du chemin de requête, et que `VACUUM`
    // exige de ne pas être dans une transaction — d'où les commits ci-dessus.
    let before = freelist_pages(conn)?;
    conn.execute_batch("VACUUM;")?;
    tracing::info!(
        freed_pages = before,
        "espace de l'archive récupéré après migration"
    );
    Ok(())
}

/// Pages allouées mais libres, en unités de `page_size`.
fn freelist_pages(conn: &Connection) -> Result<i64> {
    Ok(conn.query_row("PRAGMA freelist_count", [], |r| r.get(0))?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obs(feed_ts: i64, trip: &str, seq: i64) -> Observation {
        Observation {
            feed_ts,
            captured_at: feed_ts + 5,
            trip_id: trip.into(),
            route_id: Some("gr:tec:L1".into()),
            stop_id: "gs:tec:A".into(),
            stop_sequence: seq,
            scheduled_ms: Some(1_000),
            predicted_ms: Some(1_060),
            delay_s: Some(60),
            schedule_relationship: 0,
        }
    }

    #[test]
    fn insert_et_compte() {
        let mut a = Archive::open_in_memory().unwrap();
        assert_eq!(
            a.insert_observations(&[obs(100, "t1", 1), obs(100, "t1", 2)])
                .unwrap(),
            2
        );
        assert_eq!(a.observation_count().unwrap(), 2);
        assert_eq!(a.last_feed_ts().unwrap(), Some(100));
    }

    #[test]
    fn insert_idempotent_sur_la_cle() {
        let mut a = Archive::open_in_memory().unwrap();
        a.insert_observations(&[obs(100, "t1", 1)]).unwrap();
        // Même (feed_ts, trip_id, stop_sequence) -> remplacement, pas de doublon.
        a.insert_observations(&[obs(100, "t1", 1)]).unwrap();
        assert_eq!(a.observation_count().unwrap(), 1);
        // Nouveau feed_ts -> nouvelle ligne.
        a.insert_observations(&[obs(130, "t1", 1)]).unwrap();
        assert_eq!(a.observation_count().unwrap(), 2);
    }

    #[test]
    fn liens_alerte_course_dedupliques() {
        let mut a = Archive::open_in_memory().unwrap();
        let rows = vec![
            AlertTrip {
                feed_ts: 200,
                alert_id: "rs:tec:1".into(),
                trip_id: "gt:tec:t1".into(),
                start_date: Some("20260924".into()),
            },
            AlertTrip {
                feed_ts: 200,
                alert_id: "rs:tec:1".into(),
                trip_id: "gt:tec:t1".into(), // doublon -> idempotent
                start_date: None,
            },
        ];
        a.insert_alert_trips(&rows).unwrap();
        assert_eq!(a.alert_trip_count().unwrap(), 1);

        // Un nouveau cycle ne duplique pas le lien : il avance `last_seen`.
        a.insert_alert_trips(&[AlertTrip {
            feed_ts: 230,
            alert_id: "rs:tec:1".into(),
            trip_id: "gt:tec:t1".into(),
            start_date: None,
        }])
        .unwrap();
        assert_eq!(a.alert_trip_count().unwrap(), 1);
        let (first, last): (i64, i64) = a
            .conn
            .query_row(
                "SELECT first_seen, last_seen FROM rt_alert_trips
                 WHERE alert_id = 'rs:tec:1' AND trip_id = 'gt:tec:t1'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!((first, last), (200, 230));
    }

    fn alert(feed_ts: i64, route_ids: &str, stop_ids: &str) -> AlertRecord {
        AlertRecord {
            feed_ts,
            captured_at: feed_ts + 5,
            alert_id: "rs:tec:1".into(),
            agency_id: Some("tec".into()),
            effect: Some(1),
            severity: Some(3),
            cause: Some(2),
            route_ids: route_ids.into(),
            stop_ids: stop_ids.into(),
            header_fr: Some("Annulations".into()),
            description_fr: Some("Ligne 1".into()),
            active_from: Some(1),
            active_to: Some(2),
        }
    }

    #[test]
    fn alertes_dedupliques_et_perimetre_journalise() {
        let mut a = Archive::open_in_memory().unwrap();
        assert_eq!(a.insert_alerts(&[alert(200, "r1", "s1")]).unwrap(), 1);
        assert_eq!(a.alert_count().unwrap(), 1);
        assert_eq!(a.alert_count_current().unwrap(), 1);
        // Première sighting : le perimetre initial entre dans l'historique.
        assert_eq!(a.alert_scope_count().unwrap(), 1);

        // 20 cycles plus tard, toujours le meme perimetre -> toujours 1 ligne.
        for ts in (210..=600).step_by(30) {
            a.insert_alerts(&[alert(ts, "r1", "s1")]).unwrap();
        }
        assert_eq!(a.alert_count().unwrap(), 1);
        assert_eq!(a.alert_scope_count().unwrap(), 1);
        assert_eq!(a.alert_count_current().unwrap(), 1);
        // `first_seen` fige le premier passage, `last_seen` suit le dernier.
        let (first, last): (i64, i64) = a
            .conn
            .query_row(
                "SELECT first_seen, last_seen FROM rt_alerts WHERE alert_id = 'rs:tec:1'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(first, 200);
        assert_eq!(last, 600);

        // TEC elargit le perimetre -> une entree d'historique en plus.
        a.insert_alerts(&[alert(630, "r1", "s1,s2")]).unwrap();
        assert_eq!(a.alert_scope_count().unwrap(), 2);
        // Le perimetre au cycle 500 reste l'ancien : la timeline se relit.
        let stops_at_500: String = a
            .conn
            .query_row(
                "SELECT stop_ids FROM rt_alert_scopes
                 WHERE alert_id = 'rs:tec:1' AND since_ts <= 500
                 ORDER BY since_ts DESC LIMIT 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(stops_at_500, "s1");
    }

    #[test]
    fn alerte_absente_du_feed_courant_n_est_plus_lue() {
        let mut a = Archive::open_in_memory().unwrap();
        let mut gone = alert(200, "r1", "s1");
        gone.alert_id = "rs:tec:2".into();
        a.insert_alerts(&[alert(200, "r1", "s1"), gone]).unwrap();
        assert_eq!(a.alert_count_current().unwrap(), 2);

        // Cycle suivant : seule la premiere alerte est encore annoncee. Son
        // `last_seen` doit permettre de l'identifier comme seule courante.
        a.insert_alerts(&[alert(230, "r1", "s1")]).unwrap();
        assert_eq!(a.alert_count().unwrap(), 2); // les deux restent connues
        assert_eq!(a.alert_count_current().unwrap(), 1);
    }

    /// La migration doit répondre **exactement comme l'ancien schéma** aux
    /// requêtes de l'application, au même instant.
    ///
    /// C'est le test qui compte : l'aplatissement ne doit changer ni le nombre
    /// d'alertes courantes, ni le contenu retenu (on prend la copie la plus
    /// récente), ni l'ensemble des courses annulées.
    #[test]
    fn migration_repond_comme_l_ancien_schema() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("archive.sqlite");
        // Trois cycles : le dernier elargit le perimetre de l'alerte 1.
        let cycles: &[(i64, &str, &str)] =
            &[(200, "r1", "s1"), (230, "r1", "s1"), (260, "r1", "s1,s2")];
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE rt_alerts (
                     feed_ts INTEGER NOT NULL, captured_at INTEGER NOT NULL,
                     alert_id TEXT NOT NULL, agency_id TEXT, effect INTEGER,
                     severity INTEGER, cause INTEGER, route_ids TEXT, stop_ids TEXT,
                     header_fr TEXT, description_fr TEXT, active_from INTEGER,
                     active_to INTEGER, PRIMARY KEY (feed_ts, alert_id));
                 CREATE TABLE rt_alert_trips (
                     feed_ts INTEGER NOT NULL, alert_id TEXT NOT NULL,
                     trip_id TEXT NOT NULL, start_date TEXT,
                     PRIMARY KEY (feed_ts, alert_id, trip_id));",
            )
            .unwrap();
            for (ts, routes, stops) in cycles {
                conn.execute(
                    "INSERT INTO rt_alerts VALUES
                     (?1, ?1, 'rs:tec:1', 'tec', 1, 3, 2, ?2, ?3, 'Annulations', 'Ligne 1', 1, 2)",
                    params![ts, routes, stops],
                )
                .unwrap();
                conn.execute(
                    "INSERT INTO rt_alert_trips VALUES (?1, 'rs:tec:1', 'gt:tec:t1', NULL)",
                    params![ts],
                )
                .unwrap();
            }
            // Alerte 3 : presente aux deux premiers cycles, absente du dernier.
            for (ts, _, _) in &cycles[..2] {
                conn.execute(
                    "INSERT INTO rt_alerts VALUES
                     (?1, ?1, 'rs:tec:3', 'tec', 1, 3, 2, 'r9', 's9', 'Travaux', 'Deviation', 1, 2)",
                    params![ts],
                )
                .unwrap();
            }
        }

        // --- Ce que l'ancien schema repondait, avant migration. ---
        let (old_alerts, old_routes, old_trips): (Vec<String>, Vec<String>, Vec<String>) =
            {
                let conn = Connection::open(&path).unwrap();
                let q = |sql: &str| -> Vec<String> {
                    conn.prepare(sql)
                        .unwrap()
                        .query_map([], |r| r.get(0))
                        .unwrap()
                        .map(Result::unwrap)
                        .collect()
                };
                (
                q("SELECT alert_id FROM rt_alerts
                   WHERE feed_ts = (SELECT MAX(feed_ts) FROM rt_alerts) ORDER BY alert_id"),
                q("SELECT alert_id || '|' || route_ids || '|' || COALESCE(stop_ids,'') || '|' ||
                         COALESCE(header_fr,'') || '|' || COALESCE(description_fr,'')
                   FROM rt_alerts
                   WHERE feed_ts = (SELECT MAX(feed_ts) FROM rt_alerts) ORDER BY alert_id"),
                q("SELECT at.alert_id || '|' || at.trip_id
                   FROM rt_alert_trips at
                   JOIN rt_alerts a ON a.alert_id = at.alert_id AND a.feed_ts = at.feed_ts
                   WHERE at.feed_ts = (SELECT MAX(feed_ts) FROM rt_alerts)
                   ORDER BY at.alert_id, at.trip_id"),
            )
            };
        // Trois cycles, alerte 3 disparue du dernier : elle n'est plus courante.
        assert_eq!(old_alerts, vec!["rs:tec:1"]);

        // --- Ce que le nouveau schema repond, apres migration. ---
        let a = Archive::open(&path).unwrap();
        let q = |sql: &str| -> Vec<String> {
            a.conn
                .prepare(sql)
                .unwrap()
                .query_map([], |r| r.get(0))
                .unwrap()
                .map(Result::unwrap)
                .collect()
        };
        let new_alerts = q("SELECT alert_id FROM rt_alerts
             WHERE last_seen = (SELECT MAX(last_seen) FROM rt_alerts) ORDER BY alert_id");
        let new_routes = q(
            "SELECT alert_id || '|' || route_ids || '|' || COALESCE(stop_ids,'') || '|' ||
                     COALESCE(header_fr,'') || '|' || COALESCE(description_fr,'')
             FROM rt_alerts
             WHERE last_seen = (SELECT MAX(last_seen) FROM rt_alerts) ORDER BY alert_id",
        );
        let new_trips = q("SELECT at.alert_id || '|' || at.trip_id
             FROM rt_alert_trips at
             JOIN rt_alerts a ON a.alert_id = at.alert_id
             WHERE at.last_seen = a.last_seen
               AND a.last_seen = (SELECT MAX(last_seen) FROM rt_alerts)
             ORDER BY at.alert_id, at.trip_id");

        assert_eq!(new_alerts, old_alerts, "alertes courantes");
        assert_eq!(new_routes, old_routes, "contenu des alertes courantes");
        assert_eq!(new_trips, old_trips, "courses annoncees annulees");
        // Le contenu retenu est bien celui du dernier cycle.
        assert!(old_routes[0].contains("s1,s2"), "{}", old_routes[0]);
        // L'historique conserve les trois etats de perimetre.
        assert_eq!(a.alert_scope_count().unwrap(), 3);
        // Les liens alerte-course sont dedupliques.
        assert_eq!(a.alert_trip_count().unwrap(), 1);
        // Base versionnee jusqu'a la derniere etape, espace rendu au systeme.
        let version: i64 = a
            .conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
        assert_eq!(freelist_pages(&a.conn).unwrap(), 0);
    }

    /// Aucun index secondaire sur `rt_observations` ne doit exister tant que les
    /// statistiques par arrêt ne sont pas écrites.
    ///
    /// Ce test est là pour que la suppression ne soit pas annulée par réflexe.
    /// Le gain est mesuré (7,4 Mo réécrits à chaque insertion pour une requête
    /// qui ne les utilise pas), donc en réajouter un doit être un acte
    /// conscient — et passer par la modification de ce test.
    #[test]
    fn rt_observations_na_porte_quun_index_secondaire() {
        let a = Archive::open_in_memory().unwrap();
        let indexes: Vec<String> = a
            .conn
            .prepare(
                "SELECT name FROM sqlite_master
                 WHERE type = 'index' AND tbl_name = 'rt_observations' AND name NOT LIKE 'sqlite_%'
                 ORDER BY name",
            )
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert!(
            indexes.is_empty(),
            "index inattendu sur rt_observations : {indexes:?} \
             - verifier EXPLAIN QUERY PLAN avant d'en ajouter un"
        );
    }

    /// La migration 2 -> 3 retire les index sans toucher aux donnees.
    #[test]
    fn migration_2_vers_3_retire_les_index_inutilises() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("archive.sqlite");
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE rt_observations (
                     feed_ts INTEGER NOT NULL, captured_at INTEGER NOT NULL,
                     trip_id TEXT NOT NULL, route_id TEXT, stop_id TEXT NOT NULL,
                     stop_sequence INTEGER NOT NULL, scheduled_ms INTEGER,
                     predicted_ms INTEGER, delay_s INTEGER, schedule_relationship INTEGER NOT NULL,
                     PRIMARY KEY (feed_ts, trip_id, stop_sequence));
                 CREATE INDEX idx_obs_stop_ts  ON rt_observations(stop_id, feed_ts);
                 CREATE INDEX idx_obs_route_ts ON rt_observations(route_id, feed_ts);
                 CREATE INDEX idx_obs_trip     ON rt_observations(trip_id);",
            )
            .unwrap();
            conn.pragma_update(None, "user_version", 2).unwrap();
        }
        let mut a = Archive::open(&path).unwrap();
        let left: i64 = a
            .conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'index' AND tbl_name = 'rt_observations' AND name NOT LIKE 'sqlite_%'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(left, 0, "index encore presents");
        // La base est montee jusqu'a la derniere version, pas seulement 2 -> 3.
        let version: i64 = a
            .conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
        // Les donnees sont intactes : on n'a touche qu'aux index.
        assert_eq!(a.insert_observations(&[obs(100, "t1", 1)]).unwrap(), 1);
        assert_eq!(a.observation_count().unwrap(), 1);
    }
}
