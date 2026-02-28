# converter

Конвертер принимает уже спарсенные товары из `receiver`, нормализует поля и готовит запись для `catalog`.

## Что реализовано

- Общий `BaseParserHandler` (мастер-класс) с единым контрактом нормализации.
- Реестр обработчиков `HandlerRegistry` для выбора модуля по `parser_name`.
- Отдельный модуль `parsers/fixprice` с парсером title и `FixPriceHandler`.
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
  pipeline.py        # orchestration
```

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
- если колонка отсутствует, адаптер выбрасывает ошибку и требует применить ручные миграции `receiver` от `2026-02-26`.

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
  --batch-size 250
```

Полный sync `receiver -> catalog` (MySQL):

```bash
pip install pymysql
python3 sync_receiver_to_catalog.py \
  --receiver-db 'mysql+pymysql://user:pass@127.0.0.1:3306/receiver' \
  --catalog-db 'mysql+pymysql://user:pass@127.0.0.1:3306/catalog' \
  --parser-name fixprice \
  --batch-size 250
```

## Как расширять

1. Создать папку `converter/parsers/<parser_name>/`.
2. Реализовать `<ParserName>Handler(BaseParserHandler)`.
3. Зарегистрировать в `converter/parsers/__init__.py`.
4. При необходимости добавить parser-specific normalizers/patterns.

## Смежные проекты

- `../dataclass`
- `../storage`
- `../receiver`
