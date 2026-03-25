BEGIN;

DROP INDEX IF EXISTS ix_catalog_settlements_geo_point_gist;

ALTER TABLE catalog_settlements
    DROP COLUMN IF EXISTS geo_point;

COMMIT;
