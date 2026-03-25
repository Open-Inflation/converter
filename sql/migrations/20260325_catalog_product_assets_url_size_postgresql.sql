BEGIN;

ALTER TABLE catalog_product_assets
    ADD COLUMN IF NOT EXISTS size BIGINT NULL;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'catalog_product_assets'
          AND column_name = 'value'
    ) AND NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'catalog_product_assets'
          AND column_name = 'url'
    ) THEN
        ALTER TABLE catalog_product_assets
            RENAME COLUMN value TO url;
    END IF;
END $$;

ALTER TABLE catalog_product_assets
    ADD COLUMN IF NOT EXISTS url TEXT NULL;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'catalog_product_assets'
          AND column_name = 'value'
    ) THEN
        EXECUTE $sql$
            UPDATE catalog_product_assets
            SET url = value
            WHERE url IS NULL
              AND value IS NOT NULL
        $sql$;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM catalog_product_assets
        WHERE url IS NULL
        LIMIT 1
    ) THEN
        RAISE EXCEPTION 'catalog_product_assets.url contains NULL rows; migration refuses to invent URLs';
    END IF;
END $$;

ALTER TABLE catalog_product_assets
    ALTER COLUMN url SET NOT NULL;

ALTER TABLE catalog_product_assets
    DROP CONSTRAINT IF EXISTS uq_catalog_product_assets_slot;

ALTER TABLE catalog_product_assets
    DROP COLUMN IF EXISTS asset_kind;

ALTER TABLE catalog_product_assets
    DROP COLUMN IF EXISTS value;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'uq_catalog_product_assets_slot'
    ) THEN
        ALTER TABLE catalog_product_assets
            ADD CONSTRAINT uq_catalog_product_assets_slot
            UNIQUE (product_id, sort_order);
    END IF;
END $$;

DROP TYPE IF EXISTS catalog_product_asset_kind_enum;

COMMIT;
