# converter

Конвертер принимает уже спарсенные товары из `receiver`, нормализует поля и готовит запись для `catalog`.

## Что реализовано

- Общий `BaseParserHandler` (мастер-класс) с единым контрактом нормализации.
- Реестр обработчиков `HandlerRegistry` для выбора модуля по `parser_name`.
- Отдельные модули `parsers/fixprice`, `parsers/chizhik`, `parsers/perekrestok` с parser-specific title обработчиками.
- Пайплайн `ConverterPipeline`:
  - обработчик парсера,
  - резолв canonical product id (`plu/sku/source_id + parser`, fallback по normalized title только если нет `plu/sku`),
  - persistent image dedup,
  - backfill `NULL` полей по ближайшей версии товара во времени.

## Архитектура

```text
converter/
  core/
    base.py          # мастер-класс обработчика
    models.py        # raw/normalized dataclass-модели
    ports.py         # интерфейсы receiver/catalog/storage
    registry.py      # реестр обработчиков
    services.py      # identity, image dedup, null-backfill
  parsers/
    fixprice/
      handler.py     # обработчик Fix Price
      title_parser.py
      normalizers.py
      patterns.py
    chizhik/
      handler.py     # обработчик Чижик
      title_parser.py
      patterns.py
    perekrestok/
      handler.py     # обработчик Перекрёсток
      title_parser.py
  sync.py            # сервис batch-sync receiver -> catalog
  daemon.py          # polling daemon (receiver -> catalog)
  pipeline.py        # title/category/geo/composition normalization
```

## Catalog schema (бережный перенос)

`catalog` теперь хранит данные не только в projection-таблице, а в нормализованной структуре с историей:

- `catalog_product_snapshots` - append-only снапшоты по волатильным полям:
  `price/discount_price/loyal_price/price_unit`, `available_count`,
  плюс event-контракт (`source_event_uid`, `content_fingerprint`, `valid_from_at/valid_to_at`, `observed_at`, `created_at`).
- `catalog_product_sources` - состояние источника `(parser_name, source_id)` и ссылка на последний snapshot.
- `catalog_settlements` - справочник населенных пунктов/регионов/стран.
- `catalog_stores` - справочник магазинов из `receiver_artifact`, включая `rating`, `reviews_count`, `open_date`.
- `catalog_categories` - справочник категорий (uid/title/depth/parent + adult/icon/banner).
- `catalog_products` - текущая проекция (read-model) для быстрых чтений.
- `catalog_product_groups` - связи "товар -> группа одинаковых товаров"; `converter` пишет туда `source=converter`, а `group_uid` сейчас совпадает с вычисленным `canonical_product_id`.
- `catalog_product_assets` - текущие image assets товара (`url`, `size`, `fingerprint`, `sort_order`) в нормализованном виде.

`catalog_settlements` теперь мерджится между источниками: если совпадают `name_normalized` и `settlement_type`, а `region_normalized` и `country_normalized` совпадают или отсутствуют у одной из сторон, `converter` дополняет существующую запись вместо создания дубля.

Для title в БД хранится единое поле `title_normalized_no_stopwords`; поля
`title_normalized` и `title_original_no_stopwords` в `catalog_products` и
`catalog_product_snapshots` не сохраняются.
Для бренда в `catalog_products` хранятся оба поля: исходный `brand` и
`brand_normalized`, где нормализация сейчас намеренно простая: `strip().lower()`.

Converter сохраняет расширенный product-контракт в `catalog_products` (current projection),
а snapshot-историю ведет через единую таблицу `catalog_product_snapshots`.
Legacy snapshot-схема не поддерживается: миграция one-way удаляет устаревшие snapshot-таблицы и лишние snapshot-поля.
Автоматической миграции в `CatalogRepository` больше нет: запуск migration выполняется вручную отдельной командой.
Для добавленных store-полей в существующую PostgreSQL БД нужна миграция
`sql/migrations/20260325_catalog_store_metadata_postgresql.sql`.
Для нового контракта image assets в существующую PostgreSQL БД нужна миграция
`sql/migrations/20260325_catalog_product_assets_url_size_postgresql.sql`.
После неё для объединения fingerprint registry с asset-таблицей нужна миграция
`sql/migrations/20260325_catalog_product_assets_merge_fingerprints_postgresql.sql`.
Для таблицы групп одинаковых товаров в существующую PostgreSQL БД нужна миграция
`sql/migrations/20260325_catalog_product_groups_postgresql.sql`.
Для поля `catalog_products.brand_normalized` в существующую PostgreSQL БД нужна миграция
`sql/migrations/20260325_catalog_product_brand_normalized_postgresql.sql`.

Целевая production-СУБД: PostgreSQL (`postgresql+psycopg://...`).
SQLite оставлен только для локальных тестов/фикстур.

Политика обновления:

- история не удаляется и не перезаписывается (`append-only snapshots`);
- справочники (`settlements/categories`) пополняются и дополняются;
- `catalog_products` обновляется неразрушительно: `NULL/пустые` новые значения не затирают заполненные старые.

## Fix Price handler

Поддержан паттерн вида:

`Название, Бренд(опц), floatXfloat[ Xfloat ] см ИЛИ float (г/кг/мл/л), int(кол-во, опц), в ассортименте`

Из title формируются:

- `title_original`
- `title_normalized`
- `title_original_no_stopwords`
- `title_normalized_no_stopwords`
- `unit`, `available_count`, `package_quantity`, `package_unit`
- `dimension_height_m`, `dimension_width_m`, `dimension_depth_m` (в метрах)
- placeholder-бренд `No name` из Fix Price гасится в `NULL`, а не сохраняется как реальный бренд

Unit guide:

- `Chocolate 200 g` -> `unit=PCE`, `available_count=15`, `package_quantity=0.2`, `package_unit=KGM`
- `Milk 1 L` -> `unit=PCE`, `available_count=10`, `package_quantity=1`, `package_unit=LTR`
- `Potatoes by weight` -> `unit=KGM`, `available_count=None`, `package_quantity=None`
- `Water vending` -> `unit=LTR`, `available_count=None`, `package_quantity=None`

## Запуск демо

```bash
python3 example_fixprice_title_parser.py
```

## Тесты

```bash
python3 -m unittest discover -s tests -p 'test_*.py' -v
```

## Интеграция с receiver

Есть адаптер под SQLite-базу `receiver`:

- `converter.adapters.ReceiverSQLiteRepository`
- поддерживает только актуальную схему `receiver` (`run_artifacts.parser_name` обязателен).
- store metadata из `run_artifacts` (`rating`, `reviews_count`, `open_date`) переносится в `catalog_stores`.
- `receiver_artifact.open_date` конвертируется в `catalog_stores.open_date` только если это реальная ISO-дата; значения вроде `Скоро открытие!` не домысливаются и сохраняются как `NULL`.
- если обязательной колонки нет, адаптер падает с ошибкой несовместимой схемы.

Есть sink под SQLite-базу `catalog`:

- `converter.adapters.CatalogSQLiteRepository`
- выполняет `upsert` нормализованных товаров;
- хранит persistent `canonical_product_id` map; fingerprint изображений теперь лежит прямо в `catalog_product_assets`.

Полный sync `receiver -> catalog` (SQLite):

```bash
python3 sync_receiver_to_catalog.py \
  --receiver-db ../receiver/data/receiver.db \
  --catalog-db ./data/catalog.db \
  --parser-name fixprice \
  --receiver-fetch-size 2000 \
  --write-chunk-size 1000 \
  --sync-version v2
```

Полный sync `receiver -> catalog` (PostgreSQL):

```bash
pip install -r requirements.txt
python3 sync_receiver_to_catalog.py \
  --receiver-db 'postgresql+psycopg://user:pass@127.0.0.1:5432/receiver' \
  --catalog-db 'postgresql+psycopg://user:pass@127.0.0.1:5432/catalog' \
  --parser-name fixprice \
  --receiver-fetch-size 2000 \
  --write-chunk-size 1000 \
  --sync-version v2
```

### Очистка дублей изображений в storage

Конвертер удаляет duplicate image URLs через async outbox:

- `CONVERTER_STORAGE_BASE_URL` (или `STORAGE_BASE_URL`) — базовый URL storage.
- `CONVERTER_STORAGE_API_TOKEN` (или `STORAGE_API_TOKEN`) — токен `Bearer`.
- `CONVERTER_STORAGE_DELETE_TIMEOUT_SEC` — timeout `DELETE` запроса (по умолчанию `10`).
- ошибка удаления больше не прерывает `apply_chunk`, обработка идет через retry в outbox worker.
- перед dedup конвертер вызывает `POST /api/images/{image_name}/persist?overwrite=true` (best effort) и сохраняет новый `Location` URL.
- для заполнения `catalog_product_assets.size` конвертер делает `HEAD` на публичный image URL и читает стандартный `Content-Length`.

Удаление и HEAD-size lookup выполняются только для URL текущего storage origin и путей `/images/<name>` / `/images-permanent/<name>`.

### Демон (polling)

Запуск daemon-процесса в режиме циклического опроса `receiver`:

```bash
python3 converter_daemon.py \
  --receiver-db ../receiver/data/receiver.db \
  --catalog-db ./data/catalog.db \
  --parser-name fixprice \
  --receiver-fetch-size 2000 \
  --write-chunk-size 1000 \
  --sync-version v2 \
  --writer-mode pg_v2 \
  --poll-interval-sec 5
```

`write_chunk_size` задаёт атомарный размер `apply_chunk`.
Демон не использует HTTP trigger и работает в режиме consume-delete: после успешной записи chunk в `catalog` удаляет обработанные `run_artifact_products` из `receiver`.

## Как расширять

1. Создать папку `converter/parsers/<parser_name>/`.
2. Реализовать `<ParserName>Handler(BaseParserHandler)`.
3. Зарегистрировать в `converter/parsers/__init__.py`.
4. При необходимости добавить parser-specific normalizers/patterns.

## Смежные проекты

- `../dataclass`
- `../storage`
- `../receiver`
