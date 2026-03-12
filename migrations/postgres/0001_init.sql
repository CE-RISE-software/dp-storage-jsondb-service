CREATE TABLE IF NOT EXISTS records (
    id TEXT PRIMARY KEY,
    model TEXT NOT NULL,
    version TEXT NOT NULL,
    payload_json JSONB NOT NULL,
    created_by_sub TEXT NULL,
    tenant_id TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS idempotency_keys (
    idempotency_key TEXT PRIMARY KEY,
    payload_hash VARCHAR(64) NOT NULL,
    record_id TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_idempotency_record
        FOREIGN KEY (record_id) REFERENCES records(id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS record_read_grants (
    id BIGSERIAL PRIMARY KEY,
    record_id TEXT NOT NULL,
    grantee_sub TEXT NULL,
    grantee_tenant_id TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_record_read_grant_record
        FOREIGN KEY (record_id) REFERENCES records(id)
        ON DELETE CASCADE
);

CREATE OR REPLACE FUNCTION set_records_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_records_updated_at ON records;
CREATE TRIGGER trg_records_updated_at
BEFORE UPDATE ON records
FOR EACH ROW
EXECUTE FUNCTION set_records_updated_at();
