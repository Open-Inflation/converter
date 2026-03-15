BEGIN;

ALTER TABLE catalog_products
    ADD COLUMN IF NOT EXISTS package_weight_gross DOUBLE PRECISION NULL,
    ADD COLUMN IF NOT EXISTS package_count DOUBLE PRECISION NULL;

ALTER TABLE catalog_product_snapshots
    DROP COLUMN IF EXISTS package_weight_gross,
    DROP COLUMN IF EXISTS package_count;

COMMIT;
