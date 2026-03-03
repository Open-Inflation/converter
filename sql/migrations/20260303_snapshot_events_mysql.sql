-- One-way manual migration to snapshot-events contract for MySQL.
-- Safe to rerun. Foreign keys are NOT created (works without REFERENCES privilege).

DROP PROCEDURE IF EXISTS migrate_catalog_snapshot_events;
DELIMITER //
CREATE PROCEDURE migrate_catalog_snapshot_events()
BEGIN
    DECLARE v_has_snapshots INT DEFAULT 0;
    DECLARE v_has_available_counts INT DEFAULT 0;
    DECLARE v_has_legacy_event_cols INT DEFAULT 0;
    DECLARE v_has_legacy_available_col INT DEFAULT 0;
    DECLARE v_has_price INT DEFAULT 0;
    DECLARE v_has_discount_price INT DEFAULT 0;
    DECLARE v_has_loyal_price INT DEFAULT 0;
    DECLARE v_has_price_unit INT DEFAULT 0;
    DECLARE v_has_snapshot_created_at INT DEFAULT 0;
    DECLARE v_has_snapshot_observed_at INT DEFAULT 0;
    DECLARE v_has_available_created_at INT DEFAULT 0;
    DECLARE v_has_latest_content_fingerprint INT DEFAULT 0;
    DECLARE v_has_source_event_uid INT DEFAULT 0;
    DECLARE v_has_content_fingerprint INT DEFAULT 0;
    DECLARE v_has_valid_from_at INT DEFAULT 0;
    DECLARE v_has_valid_to_at INT DEFAULT 0;

    -- 1) Ensure target tables exist.
    CREATE TABLE IF NOT EXISTS catalog_snapshot_events (
        id INTEGER NOT NULL AUTO_INCREMENT,
        canonical_product_id VARCHAR(36) NOT NULL,
        parser_name VARCHAR(64) NOT NULL,
        source_id VARCHAR(255) NOT NULL,
        source_run_id VARCHAR(64) NULL,
        receiver_product_id INTEGER NULL,
        receiver_artifact_id INTEGER NULL,
        receiver_sort_order INTEGER NULL,
        source_event_uid VARCHAR(191) NULL,
        content_fingerprint VARCHAR(64) NULL,
        valid_from_at DATETIME(6) NULL,
        valid_to_at DATETIME(6) NULL,
        observed_at DATETIME(6) NOT NULL,
        created_at DATETIME(6) NOT NULL,
        PRIMARY KEY (id)
    ) ENGINE=InnoDB;

    CREATE TABLE IF NOT EXISTS catalog_snapshot_available_counts (
        snapshot_id INTEGER NOT NULL,
        available_count FLOAT NULL,
        created_at DATETIME(6) NOT NULL,
        PRIMARY KEY (snapshot_id)
    ) ENGINE=InnoDB;

    -- 2) Ensure event columns/indexes exist for pre-existing table.
    SELECT COUNT(*) INTO v_has_source_event_uid
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'catalog_snapshot_events'
      AND column_name = 'source_event_uid';
    IF v_has_source_event_uid = 0 THEN
        ALTER TABLE catalog_snapshot_events ADD COLUMN source_event_uid VARCHAR(191) NULL;
    END IF;

    SELECT COUNT(*) INTO v_has_content_fingerprint
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'catalog_snapshot_events'
      AND column_name = 'content_fingerprint';
    IF v_has_content_fingerprint = 0 THEN
        ALTER TABLE catalog_snapshot_events ADD COLUMN content_fingerprint VARCHAR(64) NULL;
    END IF;

    SELECT COUNT(*) INTO v_has_valid_from_at
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'catalog_snapshot_events'
      AND column_name = 'valid_from_at';
    IF v_has_valid_from_at = 0 THEN
        ALTER TABLE catalog_snapshot_events ADD COLUMN valid_from_at DATETIME(6) NULL;
    END IF;

    SELECT COUNT(*) INTO v_has_valid_to_at
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'catalog_snapshot_events'
      AND column_name = 'valid_to_at';
    IF v_has_valid_to_at = 0 THEN
        ALTER TABLE catalog_snapshot_events ADD COLUMN valid_to_at DATETIME(6) NULL;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = 'catalog_snapshot_events'
          AND index_name = 'idx_cse_latest'
    ) THEN
        CREATE INDEX idx_cse_latest ON catalog_snapshot_events (parser_name, source_id, id);
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = 'catalog_snapshot_events'
          AND index_name = 'uq_cse_event'
    ) THEN
        CREATE UNIQUE INDEX uq_cse_event ON catalog_snapshot_events (source_event_uid);
    END IF;

    -- 3) Backfill events and available_count from legacy snapshots if needed.
    SELECT COUNT(*) INTO v_has_snapshots
    FROM information_schema.tables
    WHERE table_schema = DATABASE()
      AND table_name = 'catalog_product_snapshots';

    IF v_has_snapshots = 1 THEN
        SELECT COUNT(*) INTO v_has_legacy_event_cols
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'catalog_product_snapshots'
          AND column_name IN ('canonical_product_id', 'parser_name', 'source_id', 'observed_at', 'created_at');

        IF v_has_legacy_event_cols = 5 THEN
            INSERT INTO catalog_snapshot_events (
                id,
                canonical_product_id,
                parser_name,
                source_id,
                source_run_id,
                receiver_product_id,
                receiver_artifact_id,
                receiver_sort_order,
                source_event_uid,
                content_fingerprint,
                valid_from_at,
                valid_to_at,
                observed_at,
                created_at
            )
            SELECT
                cps.id,
                cps.canonical_product_id,
                cps.parser_name,
                cps.source_id,
                cps.source_run_id,
                cps.receiver_product_id,
                cps.receiver_artifact_id,
                cps.receiver_sort_order,
                cps.source_event_uid,
                cps.content_fingerprint,
                cps.valid_from_at,
                cps.valid_to_at,
                cps.observed_at,
                cps.created_at
            FROM catalog_product_snapshots AS cps
            LEFT JOIN catalog_snapshot_events AS cse
                ON cse.id = cps.id
            WHERE cse.id IS NULL;
        END IF;

        SELECT COUNT(*) INTO v_has_legacy_available_col
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'catalog_product_snapshots'
          AND column_name = 'available_count';

        IF v_has_legacy_available_col = 1 THEN
            SELECT COUNT(*) INTO v_has_snapshot_created_at
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = 'catalog_product_snapshots'
              AND column_name = 'created_at';

            SELECT COUNT(*) INTO v_has_snapshot_observed_at
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = 'catalog_product_snapshots'
              AND column_name = 'observed_at';

            SET @legacy_created_expr := 'UTC_TIMESTAMP(6)';
            IF v_has_snapshot_created_at = 1 AND v_has_snapshot_observed_at = 1 THEN
                SET @legacy_created_expr := 'COALESCE(cps.created_at, cps.observed_at, UTC_TIMESTAMP(6))';
            ELSEIF v_has_snapshot_created_at = 1 THEN
                SET @legacy_created_expr := 'COALESCE(cps.created_at, UTC_TIMESTAMP(6))';
            ELSEIF v_has_snapshot_observed_at = 1 THEN
                SET @legacy_created_expr := 'COALESCE(cps.observed_at, UTC_TIMESTAMP(6))';
            END IF;

            SET @sql_backfill_available := CONCAT(
                'INSERT INTO catalog_snapshot_available_counts (snapshot_id, available_count, created_at) ',
                'SELECT cps.id, cps.available_count, ', @legacy_created_expr, ' ',
                'FROM catalog_product_snapshots AS cps ',
                'INNER JOIN catalog_snapshot_events AS cse ON cse.id = cps.id ',
                'LEFT JOIN catalog_snapshot_available_counts AS csac ON csac.snapshot_id = cps.id ',
                'WHERE csac.snapshot_id IS NULL'
            );
            PREPARE stmt_backfill_available FROM @sql_backfill_available;
            EXECUTE stmt_backfill_available;
            DEALLOCATE PREPARE stmt_backfill_available;
        END IF;
    END IF;

    -- 4) Rebuild catalog_product_snapshots into price-only shape.
    IF v_has_snapshots = 1 THEN
        SELECT COUNT(*) INTO v_has_price
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'catalog_product_snapshots'
          AND column_name = 'price';
        SELECT COUNT(*) INTO v_has_discount_price
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'catalog_product_snapshots'
          AND column_name = 'discount_price';
        SELECT COUNT(*) INTO v_has_loyal_price
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'catalog_product_snapshots'
          AND column_name = 'loyal_price';
        SELECT COUNT(*) INTO v_has_price_unit
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'catalog_product_snapshots'
          AND column_name = 'price_unit';

        SET @price_expr := IF(v_has_price = 1, 'cps.price', 'NULL');
        SET @discount_expr := IF(v_has_discount_price = 1, 'cps.discount_price', 'NULL');
        SET @loyal_expr := IF(v_has_loyal_price = 1, 'cps.loyal_price', 'NULL');
        SET @unit_expr := IF(v_has_price_unit = 1, 'cps.price_unit', 'NULL');

        DROP TABLE IF EXISTS catalog_product_snapshots__new;
        CREATE TABLE catalog_product_snapshots__new (
            id INTEGER NOT NULL,
            price FLOAT NULL,
            discount_price FLOAT NULL,
            loyal_price FLOAT NULL,
            price_unit VARCHAR(32) NULL,
            PRIMARY KEY (id)
        ) ENGINE=InnoDB;

        SET @sql_rebuild_price := CONCAT(
            'INSERT INTO catalog_product_snapshots__new (id, price, discount_price, loyal_price, price_unit) ',
            'SELECT cps.id, ', @price_expr, ', ', @discount_expr, ', ', @loyal_expr, ', ', @unit_expr, ' ',
            'FROM catalog_product_snapshots AS cps ',
            'INNER JOIN catalog_snapshot_events AS cse ON cse.id = cps.id'
        );
        PREPARE stmt_rebuild_price FROM @sql_rebuild_price;
        EXECUTE stmt_rebuild_price;
        DEALLOCATE PREPARE stmt_rebuild_price;

        DROP TABLE IF EXISTS catalog_product_snapshots__old;
        RENAME TABLE
            catalog_product_snapshots TO catalog_product_snapshots__old,
            catalog_product_snapshots__new TO catalog_product_snapshots;
        DROP TABLE catalog_product_snapshots__old;
    END IF;

    -- 5) Rebuild catalog_snapshot_available_counts into final shape.
    SELECT COUNT(*) INTO v_has_available_counts
    FROM information_schema.tables
    WHERE table_schema = DATABASE()
      AND table_name = 'catalog_snapshot_available_counts';

    IF v_has_available_counts = 1 THEN
        SELECT COUNT(*) INTO v_has_available_created_at
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'catalog_snapshot_available_counts'
          AND column_name = 'created_at';

        DROP TABLE IF EXISTS catalog_snapshot_available_counts__new;
        CREATE TABLE catalog_snapshot_available_counts__new (
            snapshot_id INTEGER NOT NULL,
            available_count FLOAT NULL,
            created_at DATETIME(6) NOT NULL,
            PRIMARY KEY (snapshot_id)
        ) ENGINE=InnoDB;

        SET @available_created_expr := IF(
            v_has_available_created_at = 1,
            'COALESCE(csac.created_at, UTC_TIMESTAMP(6))',
            'UTC_TIMESTAMP(6)'
        );
        SET @sql_rebuild_available := CONCAT(
            'INSERT INTO catalog_snapshot_available_counts__new (snapshot_id, available_count, created_at) ',
            'SELECT csac.snapshot_id, csac.available_count, ', @available_created_expr, ' ',
            'FROM catalog_snapshot_available_counts AS csac ',
            'INNER JOIN catalog_snapshot_events AS cse ON cse.id = csac.snapshot_id'
        );
        PREPARE stmt_rebuild_available FROM @sql_rebuild_available;
        EXECUTE stmt_rebuild_available;
        DEALLOCATE PREPARE stmt_rebuild_available;

        DROP TABLE IF EXISTS catalog_snapshot_available_counts__old;
        RENAME TABLE
            catalog_snapshot_available_counts TO catalog_snapshot_available_counts__old,
            catalog_snapshot_available_counts__new TO catalog_snapshot_available_counts;
        DROP TABLE catalog_snapshot_available_counts__old;
    END IF;

    -- 6) Drop legacy table if present.
    DROP TABLE IF EXISTS catalog_snapshot_assets;

    -- 7) Ensure product_sources contract columns/indexes.
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = DATABASE() AND table_name = 'catalog_product_sources'
    ) THEN
        SELECT COUNT(*) INTO v_has_latest_content_fingerprint
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'catalog_product_sources'
          AND column_name = 'latest_content_fingerprint';
        IF v_has_latest_content_fingerprint = 0 THEN
            ALTER TABLE catalog_product_sources
            ADD COLUMN latest_content_fingerprint VARCHAR(64) NULL;
        END IF;

        IF NOT EXISTS (
            SELECT 1
            FROM information_schema.statistics
            WHERE table_schema = DATABASE()
              AND table_name = 'catalog_product_sources'
              AND index_name = 'idx_cps_last_seen'
        ) THEN
            CREATE INDEX idx_cps_last_seen ON catalog_product_sources (last_seen_at);
        END IF;
    END IF;
END//
DELIMITER ;

CALL migrate_catalog_snapshot_events();
DROP PROCEDURE migrate_catalog_snapshot_events;
