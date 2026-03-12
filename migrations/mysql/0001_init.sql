CREATE TABLE IF NOT EXISTS records (
    id VARCHAR(255) PRIMARY KEY,
    model VARCHAR(255) NOT NULL,
    version VARCHAR(255) NOT NULL,
    payload_json JSON NOT NULL,
    created_by_sub VARCHAR(255) NULL,
    tenant_id VARCHAR(255) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS idempotency_keys (
    idempotency_key VARCHAR(255) PRIMARY KEY,
    payload_hash VARCHAR(64) NOT NULL,
    record_id VARCHAR(255) NOT NULL,
    expires_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_idempotency_record
        FOREIGN KEY (record_id) REFERENCES records(id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS record_read_grants (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    record_id VARCHAR(255) NOT NULL,
    grantee_sub VARCHAR(255) NULL,
    grantee_tenant_id VARCHAR(255) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_record_read_grant_record
        FOREIGN KEY (record_id) REFERENCES records(id)
        ON DELETE CASCADE
);
