ALTER TABLE catalog_products
    ADD COLUMN IF NOT EXISTS brand_normalized VARCHAR(255);

UPDATE catalog_products
SET brand_normalized = LOWER(BTRIM(brand))
WHERE brand IS NOT NULL
  AND BTRIM(brand) <> ''
  AND (brand_normalized IS NULL OR BTRIM(brand_normalized) = '');
