-- One-way manual migration to unified snapshot contract for MySQL.
-- Safe to rerun. Script uses only SQL (no Python migration runner).

DROP PROCEDURE IF EXISTS migrate_catalog_snapshot_contract;
DELIMITER //
CREATE PROCEDURE migrate_catalog_snapshot_contract()
BEGIN
    DECLARE v_has_snapshots INT DEFAULT 0;
    DECLARE v_has_events INT DEFAULT 0;
    DECLARE v_has_available_counts INT DEFAULT 0;
    DECLARE v_has_sources INT DEFAULT 0;
    DECLARE v_has_products INT DEFAULT 0;

    DECLARE v_has_price INT DEFAULT 0;
    DECLARE v_has_discount_price INT DEFAULT 0;
    DECLARE v_has_loyal_price INT DEFAULT 0;
    DECLARE v_has_price_unit INT DEFAULT 0;
    DECLARE v_has_available_count INT DEFAULT 0;
    DECLARE v_has_source_event_uid INT DEFAULT 0;
    DECLARE v_has_content_fingerprint INT DEFAULT 0;
    DECLARE v_has_valid_from_at INT DEFAULT 0;
    DECLARE v_has_valid_to_at INT DEFAULT 0;
    DECLARE v_has_observed_at INT DEFAULT 0;
    DECLARE v_has_created_at INT DEFAULT 0;
    DECLARE v_has_source_run_id INT DEFAULT 0;
    DECLARE v_has_receiver_product_id INT DEFAULT 0;
    DECLARE v_has_receiver_artifact_id INT DEFAULT 0;
    DECLARE v_has_receiver_sort_order INT DEFAULT 0;
    DECLARE v_has_canonical_product_id INT DEFAULT 0;
    DECLARE v_has_parser_name INT DEFAULT 0;
    DECLARE v_has_source_id INT DEFAULT 0;

    DECLARE v_has_csac_available_count INT DEFAULT 0;
    DECLARE v_has_csac_created_at INT DEFAULT 0;

    DECLARE v_has_products_price INT DEFAULT 0;
    DECLARE v_has_products_discount_price INT DEFAULT 0;
    DECLARE v_has_products_loyal_price INT DEFAULT 0;

    -- Detect current schema state.
    SELECT COUNT(*) INTO v_has_snapshots
    FROM information_schema.tables
    WHERE table_schema = DATABASE() AND table_name = 'catalog_product_snapshots';

    SELECT COUNT(*) INTO v_has_events
    FROM information_schema.tables
    WHERE table_schema = DATABASE() AND table_name = 'catalog_snapshot_events';

    SELECT COUNT(*) INTO v_has_available_counts
    FROM information_schema.tables
    WHERE table_schema = DATABASE() AND table_name = 'catalog_snapshot_available_counts';

    -- Create canonical target table if it does not exist.
    CREATE TABLE IF NOT EXISTS catalog_product_snapshots (
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
        price DECIMAL(12,4) NULL,
        discount_price DECIMAL(12,4) NULL,
        loyal_price DECIMAL(12,4) NULL,
        price_unit VARCHAR(32) NULL,
        available_count FLOAT NULL,
        PRIMARY KEY (id)
    ) ENGINE=InnoDB;

    -- Rebuild snapshots from split schema (events + price + available) when present.
    IF v_has_events = 1 THEN
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
        SELECT COUNT(*) INTO v_has_available_count
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'catalog_product_snapshots'
          AND column_name = 'available_count';

        SET @cps_price_expr := IF(v_has_price = 1, 'cps.price', 'NULL');
        SET @cps_discount_expr := IF(v_has_discount_price = 1, 'cps.discount_price', 'NULL');
        SET @cps_loyal_expr := IF(v_has_loyal_price = 1, 'cps.loyal_price', 'NULL');
        SET @cps_unit_expr := IF(v_has_price_unit = 1, 'cps.price_unit', 'NULL');
        SET @cps_available_expr := IF(v_has_available_count = 1, 'cps.available_count', 'NULL');

        IF v_has_available_counts = 1 THEN
            SELECT COUNT(*) INTO v_has_csac_available_count
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = 'catalog_snapshot_available_counts'
              AND column_name = 'available_count';
            SELECT COUNT(*) INTO v_has_csac_created_at
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = 'catalog_snapshot_available_counts'
              AND column_name = 'created_at';
        ELSE
            SET v_has_csac_available_count = 0;
            SET v_has_csac_created_at = 0;
        END IF;

        SET @csac_available_expr := IF(v_has_csac_available_count = 1, 'csac.available_count', @cps_available_expr);
        SET @csac_created_expr := IF(
            v_has_csac_created_at = 1,
            'COALESCE(cse.created_at, csac.created_at, cse.observed_at, UTC_TIMESTAMP(6))',
            'COALESCE(cse.created_at, cse.observed_at, UTC_TIMESTAMP(6))'
        );

        DROP TABLE IF EXISTS catalog_product_snapshots__new;
        CREATE TABLE catalog_product_snapshots__new (
            id INTEGER NOT NULL,
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
            price DECIMAL(12,4) NULL,
            discount_price DECIMAL(12,4) NULL,
            loyal_price DECIMAL(12,4) NULL,
            price_unit VARCHAR(32) NULL,
            available_count FLOAT NULL,
            PRIMARY KEY (id)
        ) ENGINE=InnoDB;

        SET @sql_rebuild_from_split := CONCAT(
            'INSERT INTO catalog_product_snapshots__new (',
            'id, canonical_product_id, parser_name, source_id, source_run_id, ',
            'receiver_product_id, receiver_artifact_id, receiver_sort_order, ',
            'source_event_uid, content_fingerprint, valid_from_at, valid_to_at, ',
            'observed_at, created_at, price, discount_price, loyal_price, price_unit, available_count',
            ') ',
            'SELECT ',
            'cse.id, cse.canonical_product_id, cse.parser_name, cse.source_id, cse.source_run_id, ',
            'cse.receiver_product_id, cse.receiver_artifact_id, cse.receiver_sort_order, ',
            'cse.source_event_uid, cse.content_fingerprint, cse.valid_from_at, cse.valid_to_at, ',
            'cse.observed_at, ', @csac_created_expr, ', ',
            @cps_price_expr, ', ', @cps_discount_expr, ', ', @cps_loyal_expr, ', ', @cps_unit_expr, ', ', @csac_available_expr, ' ',
            'FROM catalog_snapshot_events AS cse ',
            IF(v_has_snapshots = 1,
                'LEFT JOIN catalog_product_snapshots AS cps ON cps.id = cse.id ',
                ''),
            IF(v_has_available_counts = 1,
                'LEFT JOIN catalog_snapshot_available_counts AS csac ON csac.snapshot_id = cse.id ',
                ''),
            'ORDER BY cse.id ASC'
        );
        PREPARE stmt_rebuild_from_split FROM @sql_rebuild_from_split;
        EXECUTE stmt_rebuild_from_split;
        DEALLOCATE PREPARE stmt_rebuild_from_split;

        DROP TABLE IF EXISTS catalog_product_snapshots__old;
        IF v_has_snapshots = 1 THEN
            RENAME TABLE
                catalog_product_snapshots TO catalog_product_snapshots__old,
                catalog_product_snapshots__new TO catalog_product_snapshots;
            DROP TABLE catalog_product_snapshots__old;
        ELSE
            RENAME TABLE
                catalog_product_snapshots__new TO catalog_product_snapshots;
        END IF;
    ELSEIF v_has_snapshots = 1 THEN
        -- Rebuild snapshots from single legacy table shape (drop non-volatile columns, normalize decimals).
        SELECT COUNT(*) INTO v_has_canonical_product_id
        FROM information_schema.columns
        WHERE table_schema = DATABASE() AND table_name = 'catalog_product_snapshots' AND column_name = 'canonical_product_id';
        SELECT COUNT(*) INTO v_has_parser_name
        FROM information_schema.columns
        WHERE table_schema = DATABASE() AND table_name = 'catalog_product_snapshots' AND column_name = 'parser_name';
        SELECT COUNT(*) INTO v_has_source_id
        FROM information_schema.columns
        WHERE table_schema = DATABASE() AND table_name = 'catalog_product_snapshots' AND column_name = 'source_id';
        SELECT COUNT(*) INTO v_has_source_run_id
        FROM information_schema.columns
        WHERE table_schema = DATABASE() AND table_name = 'catalog_product_snapshots' AND column_name = 'source_run_id';
        SELECT COUNT(*) INTO v_has_receiver_product_id
        FROM information_schema.columns
        WHERE table_schema = DATABASE() AND table_name = 'catalog_product_snapshots' AND column_name = 'receiver_product_id';
        SELECT COUNT(*) INTO v_has_receiver_artifact_id
        FROM information_schema.columns
        WHERE table_schema = DATABASE() AND table_name = 'catalog_product_snapshots' AND column_name = 'receiver_artifact_id';
        SELECT COUNT(*) INTO v_has_receiver_sort_order
        FROM information_schema.columns
        WHERE table_schema = DATABASE() AND table_name = 'catalog_product_snapshots' AND column_name = 'receiver_sort_order';
        SELECT COUNT(*) INTO v_has_source_event_uid
        FROM information_schema.columns
        WHERE table_schema = DATABASE() AND table_name = 'catalog_product_snapshots' AND column_name = 'source_event_uid';
        SELECT COUNT(*) INTO v_has_content_fingerprint
        FROM information_schema.columns
        WHERE table_schema = DATABASE() AND table_name = 'catalog_product_snapshots' AND column_name = 'content_fingerprint';
        SELECT COUNT(*) INTO v_has_valid_from_at
        FROM information_schema.columns
        WHERE table_schema = DATABASE() AND table_name = 'catalog_product_snapshots' AND column_name = 'valid_from_at';
        SELECT COUNT(*) INTO v_has_valid_to_at
        FROM information_schema.columns
        WHERE table_schema = DATABASE() AND table_name = 'catalog_product_snapshots' AND column_name = 'valid_to_at';
        SELECT COUNT(*) INTO v_has_observed_at
        FROM information_schema.columns
        WHERE table_schema = DATABASE() AND table_name = 'catalog_product_snapshots' AND column_name = 'observed_at';
        SELECT COUNT(*) INTO v_has_created_at
        FROM information_schema.columns
        WHERE table_schema = DATABASE() AND table_name = 'catalog_product_snapshots' AND column_name = 'created_at';
        SELECT COUNT(*) INTO v_has_price
        FROM information_schema.columns
        WHERE table_schema = DATABASE() AND table_name = 'catalog_product_snapshots' AND column_name = 'price';
        SELECT COUNT(*) INTO v_has_discount_price
        FROM information_schema.columns
        WHERE table_schema = DATABASE() AND table_name = 'catalog_product_snapshots' AND column_name = 'discount_price';
        SELECT COUNT(*) INTO v_has_loyal_price
        FROM information_schema.columns
        WHERE table_schema = DATABASE() AND table_name = 'catalog_product_snapshots' AND column_name = 'loyal_price';
        SELECT COUNT(*) INTO v_has_price_unit
        FROM information_schema.columns
        WHERE table_schema = DATABASE() AND table_name = 'catalog_product_snapshots' AND column_name = 'price_unit';
        SELECT COUNT(*) INTO v_has_available_count
        FROM information_schema.columns
        WHERE table_schema = DATABASE() AND table_name = 'catalog_product_snapshots' AND column_name = 'available_count';

        SET @canonical_expr := IF(v_has_canonical_product_id = 1, 'cps.canonical_product_id', 'UUID()');
        SET @parser_expr := IF(v_has_parser_name = 1, 'cps.parser_name', '\'unknown\'');
        SET @source_expr := IF(v_has_source_id = 1, 'cps.source_id', 'CONCAT(\'generated:\', cps.id)');
        SET @source_run_expr := IF(v_has_source_run_id = 1, 'cps.source_run_id', 'NULL');
        SET @receiver_product_expr := IF(v_has_receiver_product_id = 1, 'cps.receiver_product_id', 'NULL');
        SET @receiver_artifact_expr := IF(v_has_receiver_artifact_id = 1, 'cps.receiver_artifact_id', 'NULL');
        SET @receiver_sort_expr := IF(v_has_receiver_sort_order = 1, 'cps.receiver_sort_order', 'NULL');
        SET @event_uid_expr := IF(v_has_source_event_uid = 1, 'cps.source_event_uid', 'NULL');
        SET @fingerprint_expr := IF(v_has_content_fingerprint = 1, 'cps.content_fingerprint', 'NULL');
        SET @valid_from_expr := IF(v_has_valid_from_at = 1, 'cps.valid_from_at', 'NULL');
        SET @valid_to_expr := IF(v_has_valid_to_at = 1, 'cps.valid_to_at', 'NULL');
        SET @observed_expr := IF(v_has_observed_at = 1, 'cps.observed_at', 'UTC_TIMESTAMP(6)');
        SET @created_expr := IF(
            v_has_created_at = 1,
            'COALESCE(cps.created_at, cps.observed_at, UTC_TIMESTAMP(6))',
            'COALESCE(cps.observed_at, UTC_TIMESTAMP(6))'
        );
        SET @price_expr := IF(v_has_price = 1, 'cps.price', 'NULL');
        SET @discount_expr := IF(v_has_discount_price = 1, 'cps.discount_price', 'NULL');
        SET @loyal_expr := IF(v_has_loyal_price = 1, 'cps.loyal_price', 'NULL');
        SET @unit_expr := IF(v_has_price_unit = 1, 'cps.price_unit', 'NULL');
        SET @available_expr := IF(v_has_available_count = 1, 'cps.available_count', 'NULL');

        DROP TABLE IF EXISTS catalog_product_snapshots__new;
        CREATE TABLE catalog_product_snapshots__new (
            id INTEGER NOT NULL,
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
            price DECIMAL(12,4) NULL,
            discount_price DECIMAL(12,4) NULL,
            loyal_price DECIMAL(12,4) NULL,
            price_unit VARCHAR(32) NULL,
            available_count FLOAT NULL,
            PRIMARY KEY (id)
        ) ENGINE=InnoDB;

        SET @sql_rebuild_from_single := CONCAT(
            'INSERT INTO catalog_product_snapshots__new (',
            'id, canonical_product_id, parser_name, source_id, source_run_id, ',
            'receiver_product_id, receiver_artifact_id, receiver_sort_order, ',
            'source_event_uid, content_fingerprint, valid_from_at, valid_to_at, ',
            'observed_at, created_at, price, discount_price, loyal_price, price_unit, available_count',
            ') ',
            'SELECT ',
            'cps.id, ', @canonical_expr, ', ', @parser_expr, ', ', @source_expr, ', ',
            @source_run_expr, ', ', @receiver_product_expr, ', ', @receiver_artifact_expr, ', ', @receiver_sort_expr, ', ',
            @event_uid_expr, ', ', @fingerprint_expr, ', ', @valid_from_expr, ', ', @valid_to_expr, ', ',
            @observed_expr, ', ', @created_expr, ', ',
            @price_expr, ', ', @discount_expr, ', ', @loyal_expr, ', ', @unit_expr, ', ', @available_expr, ' ',
            'FROM catalog_product_snapshots AS cps ',
            'ORDER BY cps.id ASC'
        );
        PREPARE stmt_rebuild_from_single FROM @sql_rebuild_from_single;
        EXECUTE stmt_rebuild_from_single;
        DEALLOCATE PREPARE stmt_rebuild_from_single;

        DROP TABLE IF EXISTS catalog_product_snapshots__old;
        RENAME TABLE
            catalog_product_snapshots TO catalog_product_snapshots__old,
            catalog_product_snapshots__new TO catalog_product_snapshots;
        DROP TABLE catalog_product_snapshots__old;
    END IF;

    -- Drop legacy split/asset tables.
    DROP TABLE IF EXISTS catalog_snapshot_events;
    DROP TABLE IF EXISTS catalog_snapshot_available_counts;
    DROP TABLE IF EXISTS catalog_snapshot_assets;

    -- Ensure snapshot indexes/constraints.
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = 'catalog_product_snapshots'
          AND index_name = 'idx_cps_latest'
    ) THEN
        CREATE INDEX idx_cps_latest ON catalog_product_snapshots (parser_name, source_id, id);
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = 'catalog_product_snapshots'
          AND index_name = 'idx_cps_canonical'
    ) THEN
        CREATE INDEX idx_cps_canonical ON catalog_product_snapshots (canonical_product_id);
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = 'catalog_product_snapshots'
          AND index_name = 'idx_cps_fingerprint'
    ) THEN
        CREATE INDEX idx_cps_fingerprint ON catalog_product_snapshots (content_fingerprint);
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = 'catalog_product_snapshots'
          AND index_name = 'idx_cps_valid_from'
    ) THEN
        CREATE INDEX idx_cps_valid_from ON catalog_product_snapshots (valid_from_at);
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = 'catalog_product_snapshots'
          AND index_name = 'idx_cps_valid_to'
    ) THEN
        CREATE INDEX idx_cps_valid_to ON catalog_product_snapshots (valid_to_at);
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = 'catalog_product_snapshots'
          AND index_name = 'uq_cps_event'
    ) THEN
        CREATE UNIQUE INDEX uq_cps_event ON catalog_product_snapshots (source_event_uid);
    END IF;

    -- Ensure product_sources contract additions.
    SELECT COUNT(*) INTO v_has_sources
    FROM information_schema.tables
    WHERE table_schema = DATABASE() AND table_name = 'catalog_product_sources';
    IF v_has_sources = 1 THEN
        IF NOT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = 'catalog_product_sources'
              AND column_name = 'latest_content_fingerprint'
        ) THEN
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

    -- Convert money columns to DECIMAL in projection table.
    SELECT COUNT(*) INTO v_has_products
    FROM information_schema.tables
    WHERE table_schema = DATABASE() AND table_name = 'catalog_products';
    IF v_has_products = 1 THEN
        SELECT COUNT(*) INTO v_has_products_price
        FROM information_schema.columns
        WHERE table_schema = DATABASE() AND table_name = 'catalog_products' AND column_name = 'price';
        SELECT COUNT(*) INTO v_has_products_discount_price
        FROM information_schema.columns
        WHERE table_schema = DATABASE() AND table_name = 'catalog_products' AND column_name = 'discount_price';
        SELECT COUNT(*) INTO v_has_products_loyal_price
        FROM information_schema.columns
        WHERE table_schema = DATABASE() AND table_name = 'catalog_products' AND column_name = 'loyal_price';

        IF v_has_products_price = 1 THEN
            ALTER TABLE catalog_products MODIFY COLUMN price DECIMAL(12,4) NULL;
        END IF;
        IF v_has_products_discount_price = 1 THEN
            ALTER TABLE catalog_products MODIFY COLUMN discount_price DECIMAL(12,4) NULL;
        END IF;
        IF v_has_products_loyal_price = 1 THEN
            ALTER TABLE catalog_products MODIFY COLUMN loyal_price DECIMAL(12,4) NULL;
        END IF;
    END IF;

    SELECT 'catalog snapshot contract migration complete' AS migration_status;
END//
DELIMITER ;

CALL migrate_catalog_snapshot_contract();
DROP PROCEDURE migrate_catalog_snapshot_contract;
