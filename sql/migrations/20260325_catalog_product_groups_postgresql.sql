CREATE TABLE IF NOT EXISTS catalog_product_groups (
    group_uid UUID NOT NULL,
    product_id BIGINT NOT NULL REFERENCES catalog_products(id) ON DELETE CASCADE,
    source VARCHAR(64) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT pk_catalog_product_groups PRIMARY KEY (group_uid, product_id, source)
);

CREATE INDEX IF NOT EXISTS ix_catalog_product_groups_product_id
    ON catalog_product_groups (product_id);

INSERT INTO catalog_product_groups (
    group_uid,
    product_id,
    source,
    created_at
)
SELECT
    p.canonical_product_id::uuid,
    p.id,
    'converter',
    p.created_at
FROM catalog_products AS p
WHERE p.canonical_product_id IS NOT NULL
ON CONFLICT (group_uid, product_id, source) DO NOTHING;
