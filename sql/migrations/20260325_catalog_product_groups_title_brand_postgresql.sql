DELETE FROM catalog_product_groups
WHERE source = 'converter';

INSERT INTO catalog_product_groups (
    group_uid,
    product_id,
    source,
    created_at
)
SELECT
    (
        SUBSTRING(MD5('product-group|title=' || COALESCE(p.title_normalized_no_stopwords, '') || '|brand=' || COALESCE(p.brand_normalized, '')) FOR 8)
        || '-' ||
        SUBSTRING(MD5('product-group|title=' || COALESCE(p.title_normalized_no_stopwords, '') || '|brand=' || COALESCE(p.brand_normalized, '')) FROM 9 FOR 4)
        || '-' ||
        SUBSTRING(MD5('product-group|title=' || COALESCE(p.title_normalized_no_stopwords, '') || '|brand=' || COALESCE(p.brand_normalized, '')) FROM 13 FOR 4)
        || '-' ||
        SUBSTRING(MD5('product-group|title=' || COALESCE(p.title_normalized_no_stopwords, '') || '|brand=' || COALESCE(p.brand_normalized, '')) FROM 17 FOR 4)
        || '-' ||
        SUBSTRING(MD5('product-group|title=' || COALESCE(p.title_normalized_no_stopwords, '') || '|brand=' || COALESCE(p.brand_normalized, '')) FROM 21 FOR 12)
    )::uuid,
    p.id,
    'converter',
    p.created_at
FROM catalog_products AS p
WHERE p.title_normalized_no_stopwords IS NOT NULL
ON CONFLICT (group_uid, product_id, source) DO NOTHING;
