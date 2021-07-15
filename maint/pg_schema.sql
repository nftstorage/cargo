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
  RETURN NEW;
END;
$$;

CREATE OR REPLACE
	FUNCTION cargo.update_batch_dags_timestamps() RETURNS TRIGGER
    LANGUAGE plpgsql
AS $$
BEGIN
  UPDATE cargo.dags SET entry_last_updated = NOW() WHERE cargo.dags.cid_v1 IN (
    SELECT be.cid_v1 FROM cargo.batch_entries be WHERE be.batch_cid IN ( NEW.batch_cid, OLD.batch_cid )
  );
  RETURN NEW;
END;
$$;

CREATE OR REPLACE
	FUNCTION cargo.update_source_dags_timestamps() RETURNS TRIGGER
    LANGUAGE plpgsql
AS $$
BEGIN
  UPDATE cargo.dags SET entry_last_updated = NOW() WHERE cargo.dags.cid_v1 IN (
    SELECT cid_v1 FROM cargo.dag_sources WHERE source IN ( NEW.source, OLD.source )
  );
  RETURN NEW;
END;
$$;


CREATE TABLE IF NOT EXISTS cargo.dags (
  cid_v1 TEXT NOT NULL UNIQUE CONSTRAINT valid_cidv1 CHECK ( cargo.valid_cid_v1(cid_v1) ),
  size_actual BIGINT CONSTRAINT valid_actual_size CHECK ( size_actual >= 0 ),
  size_claimed BIGINT CONSTRAINT valid_claimed_size CHECK ( size_claimed >= 0 ),
  entry_created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
  entry_last_updated TIMESTAMP WITH TIME ZONE NOT NULL,
  entry_last_exported TIMESTAMP WITH TIME ZONE
);
CREATE INDEX IF NOT EXISTS dags_last_idx ON cargo.dags ( entry_last_updated, entry_last_exported );
CREATE INDEX IF NOT EXISTS dags_size_actual ON cargo.dags ( size_actual );
CREATE TRIGGER trigger_dag_insert
  BEFORE INSERT ON cargo.dags
  FOR EACH ROW
  EXECUTE PROCEDURE cargo.update_entry_timestamp()
;
CREATE TRIGGER trigger_dag_updated
  BEFORE UPDATE OF size_actual, size_claimed, entry_created ON cargo.dags
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
  source TEXT NOT NULL UNIQUE,
  details JSONB
);
CREATE TRIGGER trigger_dag_update_on_related_source_insert_delete
  AFTER INSERT OR DELETE ON cargo.sources
  FOR EACH ROW
  EXECUTE PROCEDURE cargo.update_source_dags_timestamps()
;
CREATE TRIGGER trigger_dag_update_on_related_source_change
  AFTER UPDATE ON cargo.sources
  FOR EACH ROW
  WHEN (OLD IS DISTINCT FROM NEW)
  EXECUTE PROCEDURE cargo.update_source_dags_timestamps()
;

CREATE TABLE IF NOT EXISTS cargo.dag_sources (
  cid_v1 TEXT NOT NULL REFERENCES cargo.dags ( cid_v1 ),
  cid_original TEXT NOT NULL,
  source TEXT NOT NULL REFERENCES cargo.sources ( source ),
  entry_created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
  entry_removed TIMESTAMP WITH TIME ZONE,
  CONSTRAINT singleton_source_record UNIQUE ( source, cid_original )
);
CREATE INDEX IF NOT EXISTS dag_sources_cidv1_idx ON cargo.dag_sources ( cid_v1 );
CREATE INDEX IF NOT EXISTS dag_sources_entry_removed ON cargo.dag_sources ( entry_removed );
CREATE TRIGGER trigger_dag_update_on_related_sources
  AFTER INSERT OR UPDATE OR DELETE ON cargo.dag_sources
  FOR EACH ROW
  EXECUTE PROCEDURE cargo.update_parent_dag_timestamp()
;

CREATE TABLE IF NOT EXISTS cargo.batches (
  batch_cid TEXT NOT NULL UNIQUE CONSTRAINT valid_batch_cid CHECK ( cargo.valid_cid_v1(batch_cid) ),
  piece_cid TEXT UNIQUE,
  car_size BIGINT CONSTRAINT valid_car_size CHECK ( car_size >= 0 ),
  metadata JSONB,
  entry_created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS cargo.batch_entries (
  batch_cid TEXT NOT NULL REFERENCES cargo.batches ( batch_cid ),
  cid_v1 TEXT NOT NULL REFERENCES cargo.dags ( cid_v1 ),
  datamodel_selector TEXT,
  CONSTRAINT singleton_batch_entry UNIQUE ( cid_v1, batch_cid )
);
CREATE INDEX IF NOT EXISTS batch_entries_batch_cid ON cargo.batch_entries ( batch_cid );

CREATE TABLE IF NOT EXISTS cargo.providers (
  provider TEXT NOT NULL UNIQUE,
  details JSONB
);

CREATE TABLE IF NOT EXISTS cargo.deals (
  batch_cid TEXT NOT NULL REFERENCES cargo.batches ( batch_cid ),
  provider TEXT NOT NULL REFERENCES cargo.providers ( provider ),
  status TEXT NOT NULL,
  deal_id INTEGER UNIQUE,
  epoch_start INTEGER,
  epoch_end INTEGER,
  entry_created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
  entry_last_updated TIMESTAMP WITH TIME ZONE NOT NULL
);
CREATE TRIGGER trigger_deal_insert
  BEFORE INSERT ON cargo.deals
  FOR EACH ROW
  EXECUTE PROCEDURE cargo.update_entry_timestamp()
;
CREATE TRIGGER trigger_dag_update_on_related_deal_insert_delete
  AFTER INSERT OR DELETE ON cargo.deals
  FOR EACH ROW
  EXECUTE PROCEDURE cargo.update_batch_dags_timestamps()
;
CREATE TRIGGER trigger_deal_updated
  BEFORE UPDATE OF status, epoch_start, epoch_end, deal_id, entry_created ON cargo.deals
  FOR EACH ROW
  WHEN (OLD IS DISTINCT FROM NEW)
  EXECUTE PROCEDURE cargo.update_entry_timestamp()
;
CREATE TRIGGER trigger_dag_update_on_related_deal_change
  AFTER UPDATE ON cargo.deals
  FOR EACH ROW
  WHEN (OLD.entry_last_updated != NEW.entry_last_updated)
  EXECUTE PROCEDURE cargo.update_batch_dags_timestamps()
;

CREATE OR REPLACE VIEW cargo.dags_missing_list AS (
  WITH
  latest_pin_run AS (
    SELECT MAX(entry_last_updated) AS ts FROM cargo.dags WHERE size_actual IS NOT NULL
  )
  SELECT
      ds.cid_original,
      ds.source,
      ds.entry_created
    FROM cargo.dag_sources ds, cargo.dags d
  WHERE
    ds.cid_v1 = d.cid_v1
      AND
    ds.entry_removed IS NULL
      AND
    d.size_actual IS NULL
      AND
    d.entry_created < ( SELECT ts - '30 minutes'::INTERVAL FROM latest_pin_run )
  ORDER BY ds.source, d.entry_created DESC
);

CREATE OR REPLACE VIEW cargo.dags_missing_summary AS (
  WITH
  incomplete_sources AS (
    SELECT
      source,
      COUNT(*) AS count_missing,
      MIN( entry_created ) AS oldest_missing,
      MAX( entry_created) AS newest_missing
    FROM cargo.dags_missing_list GROUP BY source
  ),
  source_details AS (
    SELECT
      si.source AS source_id,
      s.details ->> 'nickname' AS source_nick,
      s.details ->> 'name' AS source_name,
      s.details ->> 'email' AS source_email,
      s.details ->> 'dcweight' AS source_weight,
      oldest_missing,
      newest_missing,
      si.count_missing,
      ( SELECT COUNT(*) AS count_total FROM cargo.dag_sources WHERE source = s.source )
    FROM incomplete_sources si
    JOIN cargo.sources s USING (source)
  )
  SELECT *, ( 100 * count_missing::NUMERIC / count_total::NUMERIC )::NUMERIC(5,2) AS pct_missing
    FROM source_details
  ORDER BY count_missing DESC, source_id
);

CREATE OR REPLACE VIEW cargo.dag_sources_summary AS (
  WITH
    summary AS (
      SELECT
        ds.source,
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
        -- exclude anything that is a member of something else from the same source
        NOT EXISTS (
          SELECT 42 FROM cargo.refs r, cargo.dag_sources rds
          WHERE r.ref_v1 = ds.cid_v1 AND r.cid_v1 = rds.cid_v1 AND rds.source = ds.source
        )
      GROUP BY source
    ),
    summary_unaggregated AS (
      SELECT
        ds.source,
        COUNT(*) AS count_total,
        SUM(d.size_actual) AS bytes_total,
        MIN(ds.entry_created) AS oldest_dag,
        MAX(ds.entry_created) AS newest_dag
      FROM cargo.dag_sources ds
      JOIN cargo.dags d USING ( cid_v1 )
      LEFT JOIN cargo.batch_entries be USING ( cid_v1 )
      WHERE
        d.size_actual IS NOT NULL AND d.size_actual <= 34000000000
          AND
        ds.entry_removed IS NULL
          AND
        be.cid_v1 IS NULL
          AND
        -- exclude anything that is a member of something else unaggregated from the same source
        NOT EXISTS (
          SELECT 42
            FROM cargo.refs r
            JOIN cargo.dag_sources rds USING ( cid_v1 )
            LEFT JOIN cargo.batch_entries rbe USING ( cid_v1 )
            LEFT JOIN cargo.batches rb
              ON rbe.batch_cid = rb.batch_cid AND rb.metadata->>'RecordType' = 'DagAggregate UnixFS'
          WHERE
            r.ref_v1 = ds.cid_v1
              AND
            rds.source = ds.source
              AND
            rb.batch_cid IS NULL
        )
      GROUP BY source
    )
  SELECT
    su.source AS source_id,
    s.details ->> 'nickname' AS source_nick,
    s.details ->> 'name' AS source_name,
    s.details ->> 'email' AS source_email,
    s.details ->> 'dcweight' AS weight,
    su.count_total AS count_total,
    unagg.count_total AS count_unaggregated,
    unagg.oldest_dag AS oldest_unaggregated,
    unagg.newest_dag AS newest_unaggregated,
    pg_size_pretty(su.bytes_total) AS size_total,
    pg_size_pretty(unagg.bytes_total) AS size_unaggregated
  FROM summary su
  JOIN cargo.sources s USING (source)
  LEFT JOIN summary_unaggregated unagg USING ( source )
  ORDER BY weight DESC NULLS FIRST, unagg.bytes_total DESC NULLS LAST, su.source
);

CREATE OR REPLACE
	FUNCTION cargo.unaggregated_for_sources(TEXT[]) RETURNS TABLE ( source TEXT, cid_v1 TEXT, size_actual BIGINT, entry_created TIMESTAMP WITH TIME ZONE )
    LANGUAGE sql STABLE PARALLEL SAFE
AS $$
  SELECT
    ds.source,
    ds.cid_v1,
    size_actual,
    ds.entry_created
  FROM cargo.dag_sources ds
  JOIN cargo.dags USING ( cid_v1 )
  WHERE
    ds.source = ANY( $1 )
      AND
    ds.entry_removed IS NULL
      AND
    size_actual IS NOT NULL
      AND
    NOT EXISTS ( SELECT 42 FROM cargo.batch_entries be WHERE ds.cid_v1 = be.cid_v1 )
      AND
    -- exclude anything that is a member of something else unaggregated (from any source)
    NOT EXISTS (
      SELECT 42
        FROM cargo.refs r
        LEFT JOIN cargo.batch_entries rbe USING ( cid_v1 )
        LEFT JOIN cargo.batches rb
          ON rbe.batch_cid = rb.batch_cid AND rb.metadata->>'RecordType' = 'DagAggregate UnixFS'
      WHERE
        r.ref_v1 = ds.cid_v1
          AND
        rb.batch_cid IS NULL
    )
$$;
