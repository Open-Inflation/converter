# TABLES.md

Документ описывает таблицы, которые использует текущий runtime `converter`.

- Блок `catalog_*` - целевая БД каталога (создается ORM `converter`).
- Блок `run_*` / `task_runs` - таблицы БД `receiver`, которые `converter` читает/чистит.
- В PostgreSQL используются native-типы (`UUID`, `NUMERIC`, `TIMESTAMPTZ`, `ENUM`, `DATE`).
- В SQLite в тестах используются совместимые fallback-типы.

## Catalog DB

### ENUM типы

`catalog_storage_delete_status_enum`:
- `pending`
- `done`
- `failed`

### `catalog_products`

Текущая read-model проекция товара (одна строка на `(parser_name, source_id)`).

| Column | Type (PostgreSQL) | Null | Notes |
|---|---|---:|---|
| `id` | `BIGINT` | no | PK, autoincrement |
| `canonical_product_id` | `UUID` | no | Канонический ID товара |
| `parser_name` | `VARCHAR(64)` | no | Источник парсера |
| `source_id` | `VARCHAR(255)` | no | Идентификатор товара у источника |
| `plu` | `VARCHAR(128)` | yes | PLU |
| `sku` | `VARCHAR(128)` | yes | SKU |
| `title_original` | `TEXT` | no | Оригинальный title |
| `title_normalized_no_stopwords` | `TEXT` | no | Нормализованный title без stop-слов |
| `brand` | `VARCHAR(255)` | yes | Бренд |
| `brand_normalized` | `VARCHAR(255)` | yes | Бренд в lower-case (`strip().lower()`) |
| `source_page_url` | `TEXT` | yes | URL карточки |
| `description` | `TEXT` | yes | Описание |
| `producer_name` | `VARCHAR(255)` | yes | Производитель |
| `producer_country` | `VARCHAR(32)` | yes | Страна производителя |
| `expiration_date_in_days` | `INTEGER` | yes | Срок годности в днях |
| `rating` | `FLOAT` | yes | Рейтинг |
| `reviews_count` | `INTEGER` | yes | Число отзывов |
| `adult` | `BOOLEAN` | yes | 18+ |
| `is_new` | `BOOLEAN` | yes | Новинка |
| `promo` | `BOOLEAN` | yes | Промо |
| `season` | `BOOLEAN` | yes | Сезонный |
| `hit` | `BOOLEAN` | yes | Хит |
| `data_matrix` | `BOOLEAN` | yes | Флаг data matrix |
| `unit` | `VARCHAR(32)` | no | Единица измерения |
| `package_quantity` | `FLOAT` | yes | Кол-во в упаковке |
| `package_unit` | `VARCHAR(32)` | yes | Единица упаковки |
| `package_weight_gross` | `FLOAT` | yes | Вес брутто упаковки (кг) |
| `package_count` | `FLOAT` | yes | Кол-во штук в упаковке |
| `dimension_height_m` | `FLOAT` | yes | Высота в метрах |
| `dimension_width_m` | `FLOAT` | yes | Ширина в метрах |
| `dimension_depth_m` | `FLOAT` | yes | Глубина в метрах |
| `primary_category_id` | `BIGINT` | yes | Основная категория (только current-state) |
| `settlement_id` | `BIGINT` | yes | Привязка к settlement |
| `composition_original` | `TEXT` | yes | Состав (оригинал) |
| `composition_normalized` | `TEXT` | yes | Состав (нормализованный) |
| `observed_at` | `TIMESTAMPTZ` | no | Когда товар наблюдался в источнике |
| `created_at` | `TIMESTAMPTZ` | no | Когда строка создана в каталоге |
| `updated_at` | `TIMESTAMPTZ` | no | Когда строка обновлена в каталоге |

Constraints/indexes:
- `PK(id)`
- `UNIQUE(parser_name, source_id)` (`uq_catalog_products_source`)
- Indexes: `canonical_product_id`, `primary_category_id`, `settlement_id`, `observed_at`

### `catalog_product_snapshots`

История волатильных полей товара (append/reuse snapshot model).

| Column | Type (PostgreSQL) | Null | Notes |
|---|---|---:|---|
| `id` | `BIGINT` | no | PK, autoincrement |
| `canonical_product_id` | `UUID` | no | Канонический ID |
| `parser_name` | `VARCHAR(64)` | no | Парсер |
| `source_id` | `VARCHAR(255)` | no | Source ID |
| `source_run_id` | `VARCHAR(64)` | yes | Run ID из receiver payload |
| `receiver_product_id` | `BIGINT` | yes | ID продукта в receiver |
| `receiver_artifact_id` | `BIGINT` | yes | ID артефакта в receiver |
| `store_id` | `BIGINT` | yes | ID магазина из `catalog_stores` |
| `receiver_sort_order` | `BIGINT` | yes | Sort order из receiver |
| `source_event_uid` | `VARCHAR(191)` | yes | Дедуп событий snapshot |
| `content_fingerprint` | `VARCHAR(64)` | yes | Fingerprint контента snapshot |
| `valid_from_at` | `TIMESTAMPTZ` | yes | Начало валидности |
| `valid_to_at` | `TIMESTAMPTZ` | yes | Конец валидности |
| `observed_at` | `TIMESTAMPTZ` | no | Время наблюдения |
| `created_at` | `TIMESTAMPTZ` | no | Время вставки |
| `price` | `NUMERIC(12,4)` | yes | Цена |
| `discount_price` | `NUMERIC(12,4)` | yes | Цена со скидкой |
| `loyal_price` | `NUMERIC(12,4)` | yes | Лояльная цена |
| `price_unit` | `VARCHAR(32)` | yes | Единица цены |
| `available_count` | `FLOAT` | yes | Наличие |

Constraints/indexes:
- `PK(id)`
- `UNIQUE(source_event_uid)` (`uq_cps_event`)
- Indexes: `canonical_product_id`, `parser_name`, `source_id`, `store_id`, `source_event_uid`, `content_fingerprint`, `valid_from_at`, `valid_to_at`, `observed_at`, `created_at`

### `catalog_stores`

Справочник магазинов/точек продаж, которые приходят из `receiver_artifact`.

| Column | Type (PostgreSQL) | Null | Notes |
|---|---|---:|---|
| `id` | `BIGINT` | no | PK, autoincrement |
| `store_key` | `VARCHAR(191)` | no | Уникальный стабильный ключ магазина |
| `parser_name` | `VARCHAR(64)` | no | Парсер |
| `source` | `VARCHAR(64)` | yes | Источник (`artifact.source`) |
| `retail_type` | `VARCHAR(64)` | yes | Тип retail |
| `code` | `VARCHAR(128)` | yes | Код магазина |
| `address` | `TEXT` | yes | Адрес |
| `schedule_weekdays_open_from` | `VARCHAR(16)` | yes | Будни: открытие |
| `schedule_weekdays_closed_from` | `VARCHAR(16)` | yes | Будни: закрытие |
| `schedule_saturday_open_from` | `VARCHAR(16)` | yes | Суббота: открытие |
| `schedule_saturday_closed_from` | `VARCHAR(16)` | yes | Суббота: закрытие |
| `schedule_sunday_open_from` | `VARCHAR(16)` | yes | Воскресенье: открытие |
| `schedule_sunday_closed_from` | `VARCHAR(16)` | yes | Воскресенье: закрытие |
| `temporarily_closed` | `BOOLEAN` | yes | Временное закрытие |
| `longitude` | `FLOAT` | yes | Долгота |
| `latitude` | `FLOAT` | yes | Широта |
| `rating` | `FLOAT` | yes | Рейтинг магазина |
| `reviews_count` | `BIGINT` | yes | Кол-во отзывов магазина |
| `open_date` | `DATE` | yes | Дата открытия магазина, если `receiver_artifact.open_date` удалось распарсить как ISO-дату |
| `settlement_id` | `BIGINT` | yes | Связь с `catalog_settlements.id` |
| `first_seen_at` | `TIMESTAMPTZ` | no | Первое наблюдение |
| `last_seen_at` | `TIMESTAMPTZ` | no | Последнее наблюдение |
| `updated_at` | `TIMESTAMPTZ` | no | Время обновления |

Constraints/indexes:
- `PK(id)`
- `UNIQUE(store_key)`
- FK: `settlement_id -> catalog_settlements.id` (`ON DELETE SET NULL`)
- Indexes: `parser_name`, `code`, `settlement_id`, `(parser_name, source, code)`

Notes:
- `converter` не выдумывает дату открытия: строки вроде `Скоро открытие!` не конвертируются и дают `catalog_stores.open_date = NULL`.

### `catalog_product_sources`

Состояние источника `(parser_name, source_id)` для быстрых решений reuse/new snapshot.

| Column | Type (PostgreSQL) | Null | Notes |
|---|---|---:|---|
| `parser_name` | `VARCHAR(64)` | no | PK part |
| `source_id` | `VARCHAR(255)` | no | PK part |
| `canonical_product_id` | `UUID` | no | Канонический ID |
| `latest_snapshot_id` | `BIGINT` | yes | Последний snapshot |
| `latest_content_fingerprint` | `VARCHAR(64)` | yes | Fingerprint последнего snapshot |
| `first_seen_at` | `TIMESTAMPTZ` | no | Первое наблюдение |
| `last_seen_at` | `TIMESTAMPTZ` | no | Последнее наблюдение |
| `updated_at` | `TIMESTAMPTZ` | no | Время обновления source-state |

Constraints/indexes:
- `PK(parser_name, source_id)`
- Index: `canonical_product_id`

### `catalog_product_groups`

Связи "товар -> группа одинаковых товаров". Для записей, которые создаёт `converter`,
`group_uid` считается детерминированно по паре
`title_normalized_no_stopwords + brand_normalized`, а `source` равен `converter`.

| Column | Type (PostgreSQL) | Null | Notes |
|---|---|---:|---|
| `group_uid` | `UUID` | no | UID группы одинаковых товаров |
| `product_id` | `BIGINT` | no | ID товара из `catalog_products.id` |
| `source` | `VARCHAR(64)` | no | Источник записи о группировке |
| `created_at` | `TIMESTAMPTZ` | no | Когда связь была создана |

Constraints/indexes:
- `PK(group_uid, product_id, source)`
- FK: `product_id -> catalog_products.id` (`ON DELETE CASCADE`)
- Index: `product_id`

### `catalog_settlements`

Справочник населенных пунктов и региональной географии.

| Column | Type (PostgreSQL) | Null | Notes |
|---|---|---:|---|
| `id` | `BIGINT` | no | PK, autoincrement |
| `geo_key` | `VARCHAR(191)` | no | Уникальный ключ гео |
| `country` | `VARCHAR(64)` | yes | Страна |
| `country_normalized` | `VARCHAR(128)` | yes | Нормализованная страна |
| `region` | `TEXT` | yes | Регион |
| `region_normalized` | `TEXT` | yes | Нормализованный регион |
| `name` | `VARCHAR(255)` | yes | Название settlement |
| `name_normalized` | `VARCHAR(255)` | yes | Нормализованное название |
| `settlement_type` | `VARCHAR(32)` | yes | Тип settlement |
| `alias` | `VARCHAR(255)` | yes | Алиас |
| `latitude` | `FLOAT` | yes | Широта |
| `longitude` | `FLOAT` | yes | Долгота |
| `first_seen_at` | `TIMESTAMPTZ` | no | Первое наблюдение |
| `last_seen_at` | `TIMESTAMPTZ` | no | Последнее наблюдение |
| `updated_at` | `TIMESTAMPTZ` | no | Время обновления |

Constraints/indexes:
- `PK(id)`
- `UNIQUE(geo_key)`

Notes:
- `converter` может переиспользовать одну и ту же запись settlement между разными источниками, если совпадают `name_normalized` и `settlement_type`, а `region_normalized` и `country_normalized` совпадают или отсутствуют у одной из сторон.

### `catalog_categories`

Справочник категорий.

| Column | Type (PostgreSQL) | Null | Notes |
|---|---|---:|---|
| `id` | `BIGINT` | no | PK, autoincrement |
| `category_key` | `VARCHAR(191)` | no | Уникальный category key |
| `parser_name` | `VARCHAR(64)` | no | Парсер |
| `source_uid` | `VARCHAR(128)` | yes | UID категории у источника |
| `parent_source_uid` | `VARCHAR(128)` | yes | Родительский UID |
| `title` | `TEXT` | yes | Заголовок категории |
| `title_normalized` | `TEXT` | yes | Нормализованный заголовок |
| `alias` | `TEXT` | yes | Алиас |
| `adult` | `BOOLEAN` | yes | 18+ категория |
| `icon` | `TEXT` | yes | Иконка |
| `banner` | `TEXT` | yes | Баннер |
| `depth` | `INTEGER` | yes | Глубина |
| `sort_order` | `INTEGER` | yes | Порядок |
| `first_seen_at` | `TIMESTAMPTZ` | no | Первое наблюдение |
| `last_seen_at` | `TIMESTAMPTZ` | no | Последнее наблюдение |
| `updated_at` | `TIMESTAMPTZ` | no | Время обновления |

Constraints/indexes:
- `PK(id)`
- `UNIQUE(category_key)`
- Indexes: `parser_name`, `source_uid`

### `catalog_identity_map`

Map идентичности товара: `(parser_name, identity_type, identity_value) -> canonical_product_id`.
`normalized_name` используется только как fallback для записей без `plu` и `sku`.

| Column | Type (PostgreSQL) | Null | Notes |
|---|---|---:|---|
| `parser_name` | `VARCHAR(64)` | no | PK part |
| `identity_type` | `VARCHAR(64)` | no | PK part (`plu`, `sku`, `source_id`, `normalized_name`) |
| `identity_value` | `VARCHAR(255)` | no | PK part |
| `canonical_product_id` | `UUID` | no | Канонический ID |
| `updated_at` | `TIMESTAMPTZ` | no | Время обновления соответствия |

Constraints/indexes:
- `PK(parser_name, identity_type, identity_value)`
- Index: `canonical_product_id`

### `catalog_product_assets`

Нормализованные ассеты current-state товара.

| Column | Type (PostgreSQL) | Null | Notes |
|---|---|---:|---|
| `id` | `BIGINT` | no | PK, autoincrement |
| `product_id` | `BIGINT` | no | ID из `catalog_products` |
| `sort_order` | `BIGINT` | no | Позиция в списке |
| `url` | `TEXT` | no | URL изображения |
| `size` | `BIGINT` | yes | Размер файла в байтах из `HEAD Content-Length`, если удалось получить |
| `fingerprint` | `VARCHAR(64)` | yes | SHA-256 от URL изображения; объединяет прежний registry `catalog_image_fingerprints` с asset-строками |
| `created_at` | `TIMESTAMPTZ` | no | Время создания |
| `updated_at` | `TIMESTAMPTZ` | no | Время обновления |

Constraints/indexes:
- `PK(id)`
- `UNIQUE(product_id, sort_order)` (`uq_catalog_product_assets_slot`)
- Indexes: `product_id`, `fingerprint`

### `catalog_storage_delete_outbox`

Outbox для удаления duplicate image URL в storage.

| Column | Type (PostgreSQL) | Null | Notes |
|---|---|---:|---|
| `id` | `BIGINT` | no | PK, autoincrement |
| `dedupe_key` | `VARCHAR(64)` | no | Уникальный ключ дубликата |
| `image_url` | `TEXT` | no | URL на удаление |
| `status` | `catalog_storage_delete_status_enum` | no | Статус обработки |
| `attempts` | `BIGINT` | no | Кол-во попыток |
| `enqueued_at` | `TIMESTAMPTZ` | no | Когда поставлено в outbox |
| `available_at` | `TIMESTAMPTZ` | no | Когда можно повторно обработать |
| `processed_at` | `TIMESTAMPTZ` | yes | Когда успешно/финально обработано |
| `last_error` | `TEXT` | yes | Последняя ошибка |

Constraints/indexes:
- `PK(id)`
- `UNIQUE(dedupe_key)`
- Indexes: `status`, `available_at`

## Receiver DB (contract used by converter)

Ниже таблицы `receiver`, которые `converter` читает и/или чистит в consume-delete flow.

### `run_artifacts`

| Column | Type (PostgreSQL) | Null | Notes |
|---|---|---:|---|
| `id` | `BIGINT` | no | PK |
| `run_id` | `VARCHAR(64)` | yes | Run ID |
| `source` | `VARCHAR(255)` | yes | Источник артефакта |
| `parser_name` | `VARCHAR(64)` | yes | Имя парсера |
| `retail_type` | `VARCHAR(64)` | yes | Тип ритейла |
| `code` | `VARCHAR(128)` | yes | Код точки |
| `address` | `TEXT` | yes | Адрес |
| `schedule_weekdays_open_from` | `VARCHAR(16)` | yes | График |
| `schedule_weekdays_closed_from` | `VARCHAR(16)` | yes | График |
| `schedule_saturday_open_from` | `VARCHAR(16)` | yes | График |
| `schedule_saturday_closed_from` | `VARCHAR(16)` | yes | График |
| `schedule_sunday_open_from` | `VARCHAR(16)` | yes | График |
| `schedule_sunday_closed_from` | `VARCHAR(16)` | yes | График |
| `temporarily_closed` | `BOOLEAN` | yes | Временно закрыт |
| `longitude` | `FLOAT` | yes | Долгота |
| `latitude` | `FLOAT` | yes | Широта |
| `rating` | `FLOAT` | yes | Рейтинг магазина |
| `reviews_count` | `BIGINT` | yes | Кол-во отзывов магазина |
| `open_date` | `VARCHAR(32)` | yes | Дата открытия или исходная строка от parser/receiver |
| `dataclass_validated` | `BOOLEAN` | yes | Флаг dataclass validation |
| `dataclass_validation_error` | `TEXT` | yes | Ошибка валидации |
| `ingested_at` | `TIMESTAMPTZ` | yes | Когда артефакт ingested |

### `task_runs`

`converter` использует это для накопления времени обработки run и отметки finish.

| Column | Type (PostgreSQL) | Null | Notes |
|---|---|---:|---|
| `id` | `VARCHAR(64)` | no | PK |
| `converter_elapsed_sec` | `BIGINT` | yes | Накопленное время converter в секундах |
| `finish` | `TIMESTAMPTZ` | yes | Время завершения consume-delete по run |

### `run_artifact_products`

| Column | Type (PostgreSQL) | Null | Notes |
|---|---|---:|---|
| `id` | `BIGINT` | no | PK |
| `artifact_id` | `BIGINT` | no | Связь с `run_artifacts` |
| `sku` | `VARCHAR(128)` | yes | SKU |
| `plu` | `VARCHAR(128)` | yes | PLU |
| `source_page_url` | `TEXT` | yes | URL карточки |
| `title` | `TEXT` | yes | Title |
| `description` | `TEXT` | yes | Описание |
| `adult` | `BOOLEAN` | yes | 18+ |
| `is_new` | `BOOLEAN` | yes | Новинка |
| `promo` | `BOOLEAN` | yes | Промо |
| `season` | `BOOLEAN` | yes | Сезонный |
| `hit` | `BOOLEAN` | yes | Хит |
| `data_matrix` | `BOOLEAN` | yes | Флаг data matrix |
| `composition` | `TEXT` | yes | Состав |
| `brand` | `VARCHAR(255)` | yes | Бренд |
| `producer_name` | `VARCHAR(255)` | yes | Производитель |
| `producer_country` | `VARCHAR(32)` | yes | Страна производителя |
| `expiration_date_in_days` | `BIGINT` | yes | Срок годности в днях |
| `rating` | `FLOAT` | yes | Рейтинг |
| `reviews_count` | `BIGINT` | yes | Кол-во отзывов |
| `price` | `FLOAT` | yes | Цена |
| `discount_price` | `FLOAT` | yes | Цена со скидкой |
| `loyal_price` | `FLOAT` | yes | Лояльная цена |
| `price_unit` | `VARCHAR(32)` | yes | Единица цены |
| `unit` | `VARCHAR(32)` | yes | Единица измерения |
| `available_count` | `FLOAT` | yes | Наличие |
| `package_quantity` | `FLOAT` | yes | Кол-во в упаковке |
| `package_unit` | `VARCHAR(32)` | yes | Единица упаковки |
| `categories_uid_json` | `JSONB` | yes | Категории UID |
| `main_image` | `TEXT` | yes | Главное изображение |
| `sort_order` | `BIGINT` | yes | Порядок |

### `run_artifact_categories`

| Column | Type (PostgreSQL) | Null | Notes |
|---|---|---:|---|
| `id` | `BIGINT` | no | PK |
| `artifact_id` | `BIGINT` | no | Связь с артефактом |
| `uid` | `VARCHAR(128)` | yes | UID категории |
| `parent_uid` | `VARCHAR(128)` | yes | Родительский UID |
| `alias` | `VARCHAR(255)` | yes | Алиас |
| `title` | `VARCHAR(255)` | yes | Название |
| `adult` | `BOOLEAN` | yes | 18+ категория |
| `icon` | `TEXT` | yes | Иконка |
| `banner` | `TEXT` | yes | Баннер |
| `depth` | `BIGINT` | yes | Глубина |
| `sort_order` | `BIGINT` | yes | Порядок |

### `run_artifact_administrative_units`

| Column | Type (PostgreSQL) | Null | Notes |
|---|---|---:|---|
| `id` | `BIGINT` | no | PK |
| `artifact_id` | `BIGINT` | no | Связь с артефактом |
| `settlement_type` | `VARCHAR(32)` | yes | Тип settlement |
| `name` | `VARCHAR(255)` | yes | Название settlement |
| `alias` | `VARCHAR(255)` | yes | Алиас |
| `region` | `VARCHAR(255)` | yes | Регион |
| `country` | `VARCHAR(64)` | yes | Страна |
| `longitude` | `FLOAT` | yes | Долгота |
| `latitude` | `FLOAT` | yes | Широта |

### `run_artifact_product_images`

| Column | Type (PostgreSQL) | Null | Notes |
|---|---|---:|---|
| `id` | `BIGINT` | no | PK |
| `product_id` | `BIGINT` | no | Product ID |
| `url` | `TEXT` | yes | URL изображения |
| `is_main` | `BOOLEAN` | yes | Главное изображение |
| `sort_order` | `BIGINT` | yes | Порядок |

### `run_artifact_product_meta`

| Column | Type (PostgreSQL) | Null | Notes |
|---|---|---:|---|
| `id` | `BIGINT` | no | PK |
| `product_id` | `BIGINT` | no | Product ID |
| `name` | `VARCHAR(255)` | yes | Имя атрибута |
| `alias` | `VARCHAR(255)` | yes | Алиас атрибута |
| `value_type` | `VARCHAR(32)` | yes | Тип значения |
| `value_text` | `TEXT` | yes | Значение |
| `sort_order` | `BIGINT` | yes | Порядок |

### `run_artifact_product_wholesale_prices`

| Column | Type (PostgreSQL) | Null | Notes |
|---|---|---:|---|
| `id` | `BIGINT` | no | PK |
| `product_id` | `BIGINT` | no | Product ID |
| `from_items` | `FLOAT` | yes | Порог кол-ва |
| `price` | `FLOAT` | yes | Оптовая цена |
| `sort_order` | `BIGINT` | yes | Порядок |

### `run_artifact_product_categories`

| Column | Type (PostgreSQL) | Null | Notes |
|---|---|---:|---|
| `id` | `BIGINT` | no | PK |
| `product_id` | `BIGINT` | no | Product ID |
| `category_uid` | `VARCHAR(128)` | yes | UID категории |
| `sort_order` | `BIGINT` | yes | Порядок |

## Что исключено из текущего runtime

- Историческая таблица связей snapshot->category (`catalog_product_category_links`) отключена в текущем runtime и больше не используется при записи.
- Историческая таблица гео-координат (`catalog_settlement_geodata`) удалена из runtime-контракта; используются только текущие координаты в `catalog_settlements`.
