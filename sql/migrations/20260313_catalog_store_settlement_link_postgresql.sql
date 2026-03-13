-- Adds store -> settlement link in normalized catalog schema.
-- Safe to rerun.

ALTER TABLE catalog_stores
    ADD COLUMN IF NOT EXISTS settlement_id BIGINT NULL;

CREATE INDEX IF NOT EXISTS ix_catalog_stores_settlement_id
    ON catalog_stores (settlement_id);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fk_catalog_stores_settlement_id'
    ) THEN
        ALTER TABLE catalog_stores
            ADD CONSTRAINT fk_catalog_stores_settlement_id
            FOREIGN KEY (settlement_id)
            REFERENCES catalog_settlements(id)
            ON DELETE SET NULL;
    END IF;
END $$;
