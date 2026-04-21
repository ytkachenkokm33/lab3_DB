# Лабораторна 3. Міграція бази даних

Рішення зроблене для номера списку `22`.
Категорія за правилом `22 mod 4 = 2`: `стан повітря / air quality`.

У проєкті є:
- layered architecture на Python;
- ORM-моделі на SQLAlchemy;
- міграції Alembic у 2 етапи;
- імпорт CSV у початкову або вже нормалізовану схему;
- консольний пошук погоди за країною та датою;
- підготовка під `PostgreSQL` і `MySQL`.

## Структура

- `app/domain` - сутності, політика оцінки якості повітря, контракт репозиторію.
- `app/application` - use-case сервіси для імпорту та пошуку.
- `app/infrastructure` - CSV reader, SQLAlchemy ORM, репозиторій, конфігурація БД.
- `app/presentation/cli` - консольний інтерфейс.
- `alembic/versions` - міграції.
- `REPORT.md` - коротка записка і відповіді на питання до роботи.

## Відібрані колонки

Обов'язкові типи:
- текст: `country`
- ціле число: `wind_degree`
- дробне число: `wind_kph`
- enum: `wind_direction`
- дата: `last_updated`
- час: `sunrise`

Категорія варіанта 22:
- `air_quality_carbon_monoxide`
- `air_quality_ozone`
- `air_quality_nitrogen_dioxide`
- `air_quality_sulphur_dioxide`
- `air_quality_pm25`
- `air_quality_pm10`
- `air_quality_us_epa_index`
- `air_quality_gb_defra_index`

Додатково збережені:
- `location_name`
- `timezone`
- `last_updated_time`
- `temperature_celsius`
- `condition_text`
- `humidity`

## Формула `is_outdoor_safe`

Поле `is_outdoor_safe` у таблиці `air_quality_metrics` обчислюється як `true`, якщо одночасно виконуються всі умови:
- `pm25 <= 35`
- `pm10 <= 50`
- `us_epa_index <= 3`
- `gb_defra_index <= 3`
- `carbon_monoxide <= 4400`
- `ozone <= 100`
- `nitrogen_dioxide <= 40`
- `sulphur_dioxide <= 20`

## Встановлення

```powershell
pip install -r requirements.txt
```

## Запуск через PostgreSQL

Якщо є Docker:

```powershell
docker compose up -d postgres
```

Підключення:

```powershell
$env:DATABASE_URL = "postgresql+psycopg://weather:weather@localhost:5432/weather_lab"
```

Сценарій з існуючою БД і міграцією даних:

```powershell
python -m alembic upgrade 0001_initial_weather_schema
python main.py import-csv
python -m alembic upgrade head
python main.py find-weather --country Ukraine --date 2024-05-16
```

Сценарій створення з нуля одразу в цільовому стані:

```powershell
python -m alembic upgrade head
python main.py import-csv
```

Якщо Docker не встановлений, але PostgreSQL уже є локально:

```powershell
python scripts/setup_postgres.py
$env:DATABASE_URL = "postgresql+psycopg://weather:weather@localhost:5432/weather_lab"
python -m alembic upgrade head
python main.py import-csv
python main.py find-weather --country Afghanistan --date 2024-05-16
```

Скрипт `setup_postgres.py` попросить пароль користувача `postgres`, створить роль `weather` з паролем `weather` і базу `weather_lab`.

## Запуск через MySQL

```powershell
docker compose up -d mysql
$env:DATABASE_URL = "mysql+pymysql://weather:weather@localhost:3306/weather_lab"
python -m alembic upgrade head
python main.py import-csv
python main.py find-weather --country Ukraine --date 2024-05-16
```

## Локальний smoke test без серверів

За замовчуванням, якщо `DATABASE_URL` не встановлений, застосунок працює з локальним SQLite-файлом `weather_lab.db`.

```powershell
python -m alembic upgrade 0001_initial_weather_schema
python main.py import-csv --limit 20
python -m alembic upgrade head
python main.py find-weather --country Afghanistan --date 2024-05-16
```

## Команди CLI

```powershell
python main.py schema-stage
python main.py import-csv --limit 100
python main.py find-weather --country Afghanistan --date 2024-05-16
python main.py find-weather --country Ukraine --date 2024-05-16 --location Kyiv
```
