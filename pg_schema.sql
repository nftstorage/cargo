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

CREATE TABLE IF NOT EXISTS cargo.sources (
  cid_v1 TEXT NOT NULL REFERENCES cargo.dags ( cid_v1 ),
  cid_original TEXT NOT NULL,
  source TEXT NOT NULL,
  entry_created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
  entry_removed TIMESTAMP WITH TIME ZONE,
  CONSTRAINT singleton_source_record UNIQUE ( source, cid_original )
);
CREATE INDEX IF NOT EXISTS sources_cidv1_idx ON cargo.sources ( cid_v1 );
CREATE TRIGGER trigger_dag_update_on_related_sources
  AFTER INSERT OR UPDATE OR DELETE ON cargo.sources
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
CREATE TRIGGER trigger_dag_update_on_related_batch_entries
  AFTER INSERT OR UPDATE OR DELETE ON cargo.batch_entries
  FOR EACH ROW
  EXECUTE PROCEDURE cargo.update_parent_dag_timestamp()
;

CREATE TABLE IF NOT EXISTS cargo.providers (
  provider TEXT NOT NULL UNIQUE,
  details JSONB
);

CREATE TABLE IF NOT EXISTS cargo.deals (
  batch_cid TEXT NOT NULL REFERENCES cargo.batches ( batch_cid ),
  provider TEXT NOT NULL REFERENCES cargo.providers ( provider ),
  status TEXT NOT NULL,
  epoch_start INTEGER,
  epoch_end INTEGER,
  deal_id INTEGER,
  entry_created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
  entry_last_updated TIMESTAMP WITH TIME ZONE NOT NULL
);
CREATE TRIGGER trigger_dag_update_on_related_deals
  AFTER INSERT OR UPDATE OR DELETE ON cargo.deals
  FOR EACH ROW
  EXECUTE PROCEDURE cargo.update_batch_dags_timestamps()
;
CREATE TRIGGER trigger_deal_insert
  BEFORE INSERT ON cargo.deals
  FOR EACH ROW
  EXECUTE PROCEDURE cargo.update_entry_timestamp()
;
CREATE TRIGGER trigger_deal_updated
  BEFORE UPDATE OF status, epoch_start, epoch_end, deal_id, entry_created ON cargo.deals
  FOR EACH ROW
  WHEN (OLD IS DISTINCT FROM NEW)
  EXECUTE PROCEDURE cargo.update_entry_timestamp()
;
