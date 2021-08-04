CREATE SCHEMA IF NOT EXISTS cargo;

CREATE OR REPLACE
  FUNCTION cargo.valid_cid_v1(TEXT) RETURNS BOOLEAN
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
AS $$
  SELECT SUBSTRING( $1 FROM 1 FOR 3 ) = 'baf'
$$;

CREATE OR REPLACE
  FUNCTION cargo.update_entry_timestamp() RETURNS TRIGGER
    LANGUAGE plpgsql
AS $$
BEGIN
  NEW.entry_last_updated = NOW();
  RETURN NEW;
END;
$$;

CREATE OR REPLACE
  FUNCTION cargo.update_parent_dag_timestamp() RETURNS TRIGGER
    LANGUAGE plpgsql
AS $$
BEGIN
  UPDATE cargo.dags SET entry_last_updated = NOW() WHERE cargo.dags.cid_v1 IN ( NEW.cid_v1, OLD.cid_v1 );
  RETURN NULL;
END;
$$;

CREATE OR REPLACE
  FUNCTION cargo.update_aggregate_dags_timestamps() RETURNS TRIGGER
    LANGUAGE plpgsql
AS $$
BEGIN
  UPDATE cargo.dags SET entry_last_updated = NOW() WHERE cargo.dags.cid_v1 IN (
    SELECT ae.cid_v1 FROM cargo.aggregate_entries ae WHERE ae.aggregate_cid IN ( NEW.aggregate_cid, OLD.aggregate_cid )
  );
  RETURN NULL;
END;
$$;

CREATE OR REPLACE
  FUNCTION cargo.update_source_dags_timestamps() RETURNS TRIGGER
    LANGUAGE plpgsql
AS $$
BEGIN
  UPDATE cargo.dags SET entry_last_updated = NOW() WHERE cargo.dags.cid_v1 IN (
    SELECT cid_v1 FROM cargo.dag_sources WHERE srcid IN ( NEW.srcid, OLD.srcid )
  );
  RETURN NULL;
END;
$$;

CREATE OR REPLACE
  FUNCTION cargo.record_deal_event() RETURNS TRIGGER
    LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO cargo.deal_events ( deal_id, status ) VALUES ( NEW.deal_id, NEW.status );
  RETURN NULL;
END;
$$;


CREATE TABLE IF NOT EXISTS cargo.dags (
  cid_v1 TEXT NOT NULL UNIQUE CONSTRAINT valid_cidv1 CHECK ( cargo.valid_cid_v1(cid_v1) ),
  size_actual BIGINT CONSTRAINT valid_actual_size CHECK ( size_actual >= 0 ),
  entry_created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
  entry_last_updated TIMESTAMP WITH TIME ZONE NOT NULL
);
CREATE INDEX IF NOT EXISTS dags_last_updated_idx ON cargo.dags ( entry_last_updated );
CREATE INDEX IF NOT EXISTS dags_size_actual ON cargo.dags ( size_actual );
CREATE TRIGGER trigger_dag_insert
  BEFORE INSERT ON cargo.dags
  FOR EACH ROW
  EXECUTE PROCEDURE cargo.update_entry_timestamp()
;
CREATE TRIGGER trigger_dag_updated
  BEFORE UPDATE OF size_actual, entry_created ON cargo.dags
  FOR EACH ROW
  WHEN (OLD IS DISTINCT FROM NEW)
  EXECUTE PROCEDURE cargo.update_entry_timestamp()
;


CREATE TABLE IF NOT EXISTS cargo.refs (
  cid_v1 TEXT NOT NULL REFERENCES cargo.dags ( cid_v1 ),
  ref_v1 TEXT NOT NULL CONSTRAINT valid_cidv1 CHECK ( cargo.valid_cid_v1(ref_v1) ),
  CONSTRAINT singleton_refs_record UNIQUE ( cid_v1, ref_v1 )
);
CREATE INDEX IF NOT EXISTS refs_ref_v1 ON cargo.refs ( ref_v1 );


CREATE TABLE IF NOT EXISTS cargo.sources (
  srcid BIGSERIAL NOT NULL UNIQUE,
  project INTEGER NOT NULL,
  source TEXT NOT NULL,
  weight INTEGER CONSTRAINT weight_range CHECK ( weight BETWEEN -99 AND 99 ),
  details JSONB,
  entry_created TIMESTAMP WITH TIME ZONE NOT NULL,
  CONSTRAINT singleton_source_record UNIQUE ( source, project )
);
CREATE INDEX IF NOT EXISTS sources_weight ON cargo.sources ( weight );
CREATE TRIGGER trigger_dag_update_on_related_source_insert_delete
  AFTER INSERT OR DELETE ON cargo.sources
  FOR EACH ROW
  EXECUTE PROCEDURE cargo.update_source_dags_timestamps()
;
CREATE TRIGGER trigger_dag_update_on_related_source_change
  AFTER UPDATE OF weight ON cargo.sources
  FOR EACH ROW
  WHEN ( COALESCE( OLD.weight, 100) != COALESCE( NEW.weight, 100 ) )
  EXECUTE PROCEDURE cargo.update_source_dags_timestamps()
;


CREATE TABLE IF NOT EXISTS cargo.dag_sources (
  srcid BIGINT NOT NULL REFERENCES cargo.sources ( srcid ),
  cid_v1 TEXT NOT NULL REFERENCES cargo.dags ( cid_v1 ),
  source_key TEXT NOT NULL,
  details JSONB,
  entry_created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
  entry_last_updated TIMESTAMP WITH TIME ZONE NOT NULL,
  entry_removed TIMESTAMP WITH TIME ZONE,
  entry_last_exported TIMESTAMP WITH TIME ZONE,
  CONSTRAINT singleton_dag_source_record UNIQUE ( srcid, source_key )
);
CREATE INDEX IF NOT EXISTS dag_sources_cidv1_idx ON cargo.dag_sources ( cid_v1 );
CREATE INDEX IF NOT EXISTS dag_sources_entry_removed ON cargo.dag_sources ( entry_removed );
CREATE INDEX IF NOT EXISTS dag_sources_entry_created ON cargo.dag_sources ( entry_created );
CREATE TRIGGER trigger_dag_source_insert
  BEFORE INSERT ON cargo.dag_sources
  FOR EACH ROW
  EXECUTE PROCEDURE cargo.update_entry_timestamp()
;
CREATE TRIGGER trigger_dag_update_on_new_sources
  AFTER INSERT OR DELETE ON cargo.dag_sources
  FOR EACH ROW
  EXECUTE PROCEDURE cargo.update_parent_dag_timestamp()
;
CREATE TRIGGER trigger_dag_source_updated
  BEFORE UPDATE ON cargo.dag_sources
  FOR EACH ROW
  WHEN (OLD IS DISTINCT FROM NEW)
  EXECUTE PROCEDURE cargo.update_entry_timestamp()
;
CREATE TRIGGER trigger_dag_update_on_related_sources
  AFTER UPDATE OF source_key, entry_removed ON cargo.dag_sources
  FOR EACH ROW
  WHEN (OLD IS DISTINCT FROM NEW)
  EXECUTE PROCEDURE cargo.update_parent_dag_timestamp()
;


CREATE TABLE IF NOT EXISTS cargo.aggregates (
  aggregate_cid TEXT NOT NULL UNIQUE CONSTRAINT valid_aggregate_cid CHECK ( cargo.valid_cid_v1(aggregate_cid) ),
  piece_cid TEXT UNIQUE NOT NULL,
  sha256hex TEXT NOT NULL,
  payload_size BIGINT NOT NULL CONSTRAINT valid_payload_size CHECK ( payload_size > 0 ),
  metadata JSONB,
  entry_created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS cargo.aggregate_entries (
  aggregate_cid TEXT NOT NULL REFERENCES cargo.aggregates ( aggregate_cid ),
  cid_v1 TEXT NOT NULL REFERENCES cargo.dags ( cid_v1 ),
  datamodel_selector TEXT NOT NULL,
  CONSTRAINT singleton_aggregate_entry UNIQUE ( cid_v1, aggregate_cid )
);
CREATE INDEX IF NOT EXISTS aggregate_entries_aggregate_cid ON cargo.aggregate_entries ( aggregate_cid );


CREATE TABLE IF NOT EXISTS cargo.clients (
  client TEXT NOT NULL UNIQUE CONSTRAINT valid_client_id CHECK ( SUBSTRING( client FROM 1 FOR 2 ) IN ( 'f1', 'f2', 'f3' ) ),
  filp_available BIGINT NOT NULL,
  details JSONB
);


CREATE TABLE IF NOT EXISTS cargo.providers (
  provider TEXT NOT NULL UNIQUE CONSTRAINT valid_provider_id CHECK ( SUBSTRING( provider FROM 1 FOR 2 ) = 'f0' ),
  details JSONB
);


CREATE TABLE IF NOT EXISTS cargo.deals (
  deal_id BIGINT UNIQUE NOT NULL CONSTRAINT valid_id CHECK ( deal_id > 0 ),
  aggregate_cid TEXT NOT NULL REFERENCES cargo.aggregates ( aggregate_cid ),
  client TEXT NOT NULL REFERENCES cargo.clients ( client ),
  provider TEXT NOT NULL REFERENCES cargo.providers ( provider ),
  status TEXT NOT NULL,
  epoch_start INTEGER NOT NULL CONSTRAINT valid_start CHECK ( epoch_start > 0 ),
  epoch_end INTEGER NOT NULL CONSTRAINT valid_end CHECK ( epoch_end > 0 ),
  entry_created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
  entry_last_updated TIMESTAMP WITH TIME ZONE NOT NULL
);
CREATE INDEX IF NOT EXISTS deals_aggregate_cid ON cargo.deals ( aggregate_cid );
CREATE INDEX IF NOT EXISTS deals_client ON cargo.deals ( client );
CREATE INDEX IF NOT EXISTS deals_provider ON cargo.deals ( provider );
CREATE INDEX IF NOT EXISTS deals_status ON cargo.deals ( status );
CREATE TRIGGER trigger_deal_insert
  BEFORE INSERT ON cargo.deals
  FOR EACH ROW
  EXECUTE PROCEDURE cargo.update_entry_timestamp()
;
CREATE TRIGGER trigger_dag_update_on_related_deal_insert_delete
  AFTER INSERT OR DELETE ON cargo.deals
  FOR EACH ROW
  EXECUTE PROCEDURE cargo.update_aggregate_dags_timestamps()
;
CREATE TRIGGER trigger_deal_updated
  BEFORE UPDATE ON cargo.deals
  FOR EACH ROW
  WHEN (OLD IS DISTINCT FROM NEW)
  EXECUTE PROCEDURE cargo.update_entry_timestamp()
;
CREATE TRIGGER trigger_dag_update_on_related_deal_change
  AFTER UPDATE ON cargo.deals
  FOR EACH ROW
  WHEN (OLD.entry_last_updated != NEW.entry_last_updated)
  EXECUTE PROCEDURE cargo.update_aggregate_dags_timestamps()
;
CREATE TRIGGER trigger_basic_deal_history_on_insert
  AFTER INSERT ON cargo.deals
  FOR EACH ROW
  EXECUTE PROCEDURE cargo.record_deal_event()
;
CREATE TRIGGER trigger_basic_deal_history_on_update
  AFTER UPDATE ON cargo.deals
  FOR EACH ROW
  WHEN (OLD IS DISTINCT FROM NEW)
  EXECUTE PROCEDURE cargo.record_deal_event()
;


CREATE TABLE IF NOT EXISTS cargo.deal_events (
  entry_id BIGSERIAL UNIQUE NOT NULL,
  deal_id BIGINT NOT NULL REFERENCES cargo.deals( deal_id ),
  status TEXT NOT NULL,
  entry_created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS deal_events_deal_id ON cargo.deal_events ( deal_id );


CREATE OR REPLACE VIEW cargo.dags_missing_list AS (

  SELECT * FROM (

    SELECT
        s.project,
        ds.cid_v1,
        ds.source_key,
        ds.srcid,
        ds.entry_created,
        ( ds.entry_removed IS NOT NULL ) AS is_tombstone,
        ds.details
      FROM cargo.dag_sources ds
      JOIN cargo.dags d USING ( cid_v1 )
      JOIN cargo.sources s USING ( srcid )
    WHERE
      d.size_actual IS NULL

          UNION ALL

    SELECT
        s.project,
        ds.cid_v1,
        ds.source_key,
        ( SELECT srcid FROM cargo.sources ss WHERE ss.project = s.project AND ss.source = REPLACE( ds.details->>'upload_type', 'Redirect from user ', '') ) AS srcid,
        ds.entry_created,
        false AS is_tombstone,
        ds.details
      FROM cargo.dag_sources ds
      JOIN cargo.dags d USING ( cid_v1 )
      JOIN cargo.sources s USING ( srcid )
    WHERE
      d.size_actual IS NULL
        AND
      s.source = 'INTERNAL SYSTEM USER'
        AND
      ds.details->>'upload_type' LIKE 'Redirect from user%'
  ) u
  ORDER BY project, srcid, entry_created DESC
);

CREATE OR REPLACE VIEW cargo.dags_missing_summary AS (
  WITH
  incomplete_sources AS (
    SELECT
      srcid,
      COUNT(*) AS count_missing,
      MIN( entry_created ) AS oldest_missing,
      MAX( entry_created ) AS newest_missing
    FROM cargo.dags_missing_list
    WHERE NOT is_tombstone
    GROUP BY srcid
  ),
  source_details AS (
    SELECT
      s.project,
      si.srcid,
      COALESCE( s.details ->> 'nickname', s.details ->> 'github' ) AS source_nick,
      s.details ->> 'name' AS source_name,
      s.details ->> 'email' AS source_email,
      s.weight,
      oldest_missing,
      newest_missing,
      si.count_missing,
      ( SELECT COUNT(*) AS count_total FROM cargo.dag_sources WHERE srcid = s.srcid )
    FROM incomplete_sources si
    JOIN cargo.sources s USING (srcid)
  )
  SELECT *, ( 100 * count_missing::NUMERIC / count_total::NUMERIC )::NUMERIC(5,2) AS pct_missing
    FROM source_details
  ORDER BY count_missing DESC, srcid
);

CREATE OR REPLACE VIEW cargo.dag_sources_summary AS (
  WITH
    summary AS (
      SELECT
        ds.srcid,
        COUNT(*) AS count_total,
        SUM(d.size_actual) AS bytes_total,
        MIN(ds.entry_created) AS oldest_dag,
        MAX(ds.entry_created) AS newest_dag
      FROM cargo.dag_sources ds
      JOIN cargo.dags d USING ( cid_v1 )
      WHERE
        d.size_actual IS NOT NULL AND d.size_actual <= 34000000000
          AND
        ds.entry_removed IS NULL
          AND
        -- exclude anything that is a member of something else from the same srcid
        NOT EXISTS (
          SELECT 42 FROM cargo.refs r, cargo.dag_sources rds
          WHERE r.ref_v1 = ds.cid_v1 AND r.cid_v1 = rds.cid_v1 AND rds.srcid = ds.srcid
        )
      GROUP BY srcid
    ),
    summary_unaggregated AS (
      SELECT
        ds.srcid,
        COUNT(*) AS count_total,
        SUM(d.size_actual) AS bytes_total,
        MIN(ds.entry_created) AS oldest_dag,
        MAX(ds.entry_created) AS newest_dag
      FROM cargo.dag_sources ds
      JOIN cargo.dags d USING ( cid_v1 )
      LEFT JOIN cargo.aggregate_entries ae USING ( cid_v1 )
      WHERE
        d.size_actual IS NOT NULL AND d.size_actual <= 34000000000
          AND
        ds.entry_removed IS NULL
          AND
        ae.cid_v1 IS NULL
          AND
        -- exclude anything that is a member of something else unaggregated from the same srcid
        NOT EXISTS (
          SELECT 42
            FROM cargo.refs r
            JOIN cargo.dag_sources rds USING ( cid_v1 )
            LEFT JOIN cargo.aggregate_entries rae USING ( cid_v1 )
          WHERE
            r.ref_v1 = d.cid_v1
              AND
            rds.srcid = ds.srcid
              AND
            rds.entry_removed IS NULL
              AND
            rae.aggregate_cid IS NULL
        )
      GROUP BY srcid
    )
  SELECT
    s.project,
    su.srcid,
    COALESCE( s.details ->> 'nickname', s.details ->> 'github' ) AS source_nick,
    s.details ->> 'name' AS source_name,
    s.details ->> 'email' AS source_email,
    s.weight,
    su.count_total AS count_total,
    unagg.count_total AS count_unaggregated,
    unagg.oldest_dag AS oldest_unaggregated,
    unagg.newest_dag AS newest_unaggregated,
    pg_size_pretty(su.bytes_total) AS size_total,
    ( su.bytes_total::NUMERIC / ( 1::BIGINT << 30 )::NUMERIC )::NUMERIC(99,3) AS GiB_total,
    pg_size_pretty(unagg.bytes_total) AS size_unaggregated,
    ( unagg.bytes_total::NUMERIC / ( 1::BIGINT << 30 )::NUMERIC )::NUMERIC(99,3) AS GiB_unaggregated
  FROM summary su
  JOIN cargo.sources s USING ( srcid )
  LEFT JOIN summary_unaggregated unagg USING ( srcid )
  ORDER BY (unagg.bytes_total > 0 ), weight DESC NULLS FIRST, unagg.bytes_total DESC NULLS LAST, su.bytes_total DESC NULLS FIRST, su.srcid
);

CREATE OR REPLACE
  FUNCTION cargo.unaggregated_for_sources(BIGINT[]) RETURNS TABLE ( srcid BIGINT, cid_v1 TEXT, size_actual BIGINT, entry_created TIMESTAMP WITH TIME ZONE )
    LANGUAGE sql STABLE PARALLEL SAFE
AS $$
  SELECT
      ds.srcid,
      ds.cid_v1,
      size_actual,
      ds.entry_created
    FROM cargo.dag_sources ds
    JOIN cargo.dags d USING ( cid_v1 )
    LEFT JOIN cargo.aggregate_entries ae USING ( cid_v1 )
  WHERE
    ds.srcid = ANY( $1 )
      AND
    ds.entry_removed IS NULL
      AND
    d.size_actual IS NOT NULL
      AND
    ae.cid_v1 IS NULL
      AND
    -- exclude anything that is a member of something else unaggregated (from any srcid)
    NOT EXISTS (
      SELECT 42
        FROM cargo.refs r
        JOIN cargo.dag_sources rds USING ( cid_v1 )
        LEFT JOIN cargo.aggregate_entries rae USING ( cid_v1 )
      WHERE
        r.ref_v1 = ds.cid_v1
          AND
        rds.entry_removed IS NULL
          AND
        rae.aggregate_cid IS NULL
    )
$$;
