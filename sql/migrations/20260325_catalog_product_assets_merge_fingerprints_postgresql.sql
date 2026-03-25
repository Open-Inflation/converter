BEGIN;

ALTER TABLE catalog_product_assets
    ADD COLUMN IF NOT EXISTS fingerprint VARCHAR(64) NULL;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = current_schema()
          AND table_name = 'catalog_image_fingerprints'
    ) THEN
        EXECUTE $sql$
            UPDATE catalog_product_assets AS assets
            SET fingerprint = registry.fingerprint
            FROM catalog_image_fingerprints AS registry
            WHERE assets.url = registry.canonical_url
              AND assets.fingerprint IS NULL
        $sql$;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS ix_catalog_product_assets_fingerprint
    ON catalog_product_assets (fingerprint);

DROP TABLE IF EXISTS catalog_image_fingerprints;

COMMIT;
