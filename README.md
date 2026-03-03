# converter

Конвертер принимает уже спарсенные товары из `receiver`, нормализует поля и готовит запись для `catalog`.

## Что реализовано

- Общий `BaseParserHandler` (мастер-класс) с единым контрактом нормализации.
- Реестр обработчиков `HandlerRegistry` для выбора модуля по `parser_name`.
- Отдельные модули `parsers/fixprice`, `parsers/chizhik`, `parsers/perekrestok` с parser-specific title обработчиками.
- Пайплайн `ConverterPipeline`:
  - обработчик парсера,
  - резолв canonical product id (`plu/sku/source_id + parser`),
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

- `catalog_product_snapshots` - append-only история версий товара (каждый проход sync добавляет snapshot, без перетирания прошлого).
- `catalog_product_sources` - состояние источника `(parser_name, source_id)` и ссылка на последний snapshot.
- `catalog_settlements` - справочник населенных пунктов/регионов/стран.
- `catalog_settlement_geodata` - история геоточек (`lat/lon`) по settlement.
- `catalog_categories` - справочник категорий (uid/title/depth/parent).
- `catalog_product_category_links` - связи snapshot -> category.
- `catalog_products` - текущая проекция (read-model) для быстрых чтений.
- `catalog_product_assets` / `catalog_snapshot_assets` - массивные поля товара (image urls, duplicates, fingerprints) в нормализованном виде.

Для title в БД хранится единое поле `title_normalized_no_stopwords`; поля
`title_normalized` и `title_original_no_stopwords` в `catalog_products` и
`catalog_product_snapshots` не сохраняются.

Converter сохраняет расширенный product-контракт: в snapshots/current projection
пишутся цены (`price/discount_price/loyal_price/price_unit`), product-флаги и producer/rating,
оригинальный и нормализованный состав (`composition_original` / `composition_normalized`).

Политика обновления:

- история не удаляется и не перезаписывается (`append-only snapshots`);
- справочники (`settlements/categories/geodata`) пополняются и дополняются;
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
- если обязательной колонки нет, адаптер падает с ошибкой несовместимой схемы.

Есть sink под SQLite-базу `catalog`:

- `converter.adapters.CatalogSQLiteRepository`
- выполняет `upsert` нормализованных товаров;
- хранит persistent `canonical_product_id` map, image fingerprints и sync cursor.

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

Полный sync `receiver -> catalog` (MySQL):

```bash
pip install sqlalchemy pymysql pymorphy3 razdel stop-words
python3 sync_receiver_to_catalog.py \
  --receiver-db 'mysql+pymysql://user:pass@127.0.0.1:3306/receiver' \
  --catalog-db 'mysql+pymysql://user:pass@127.0.0.1:3306/catalog' \
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

Удаление выполняется только для URL текущего storage origin и путей `/images/<name>`.

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
  --writer-mode mysql_v2 \
  --poll-interval-sec 5
```

`write_chunk_size` задаёт атомарный размер `apply_chunk`.
Демон не использует HTTP trigger и самостоятельно подхватывает новые записи по cursor.

## Как расширять

1. Создать папку `converter/parsers/<parser_name>/`.
2. Реализовать `<ParserName>Handler(BaseParserHandler)`.
3. Зарегистрировать в `converter/parsers/__init__.py`.
4. При необходимости добавить parser-specific normalizers/patterns.

## Смежные проекты

- `../dataclass`
- `../storage`
- `../receiver`
