from __future__ import annotations

from html import escape
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "Звіт Лаб 3 Міграція БД.docx"


def xml_text(value: str) -> str:
    return escape(value, quote=True)


def run(text: str, *, bold: bool = False, font: str | None = None, size: int = 24) -> str:
    props: list[str] = []
    if bold:
        props.append("<w:b/>")
        props.append("<w:bCs/>")
    if font:
        props.append(
            f'<w:rFonts w:ascii="{xml_text(font)}" w:hAnsi="{xml_text(font)}" '
            f'w:cs="{xml_text(font)}"/>'
        )
    props.append(f'<w:sz w:val="{size}"/>')
    props.append(f'<w:szCs w:val="{size}"/>')
    rpr = f"<w:rPr>{''.join(props)}</w:rPr>"
    return f'<w:r>{rpr}<w:t xml:space="preserve">{xml_text(text)}</w:t></w:r>'


def paragraph(
    text: str = "",
    *,
    style: str | None = None,
    bold: bool = False,
    font: str | None = None,
    size: int = 24,
    after: int = 120,
) -> str:
    ppr_parts = [f'<w:spacing w:after="{after}"/>']
    if style:
        ppr_parts.insert(0, f'<w:pStyle w:val="{style}"/>')
    return f"<w:p><w:pPr>{''.join(ppr_parts)}</w:pPr>{run(text, bold=bold, font=font, size=size)}</w:p>"


def heading(text: str, level: int = 1) -> str:
    style = "Heading1" if level == 1 else "Heading2"
    size = 32 if level == 1 else 28
    return paragraph(text, style=style, bold=True, size=size, after=180)


def bullet(text: str) -> str:
    return paragraph("• " + text, after=80)


def code_block(text: str) -> str:
    lines = text.strip("\n").splitlines()
    blocks = []
    for line in lines:
        blocks.append(
            '<w:p><w:pPr><w:spacing w:after="0"/>'
            '<w:shd w:fill="F2F2F2"/></w:pPr>'
            f'{run(line, font="Consolas", size=20)}</w:p>'
        )
    blocks.append(paragraph("", after=120))
    return "".join(blocks)


def table(headers: list[str], rows: list[list[str]]) -> str:
    def cell(text: str, *, bold: bool = False) -> str:
        return (
            "<w:tc><w:tcPr><w:tcW w:w=\"2500\" w:type=\"dxa\"/></w:tcPr>"
            f"{paragraph(text, bold=bold, after=0)}</w:tc>"
        )

    xml = [
        "<w:tbl>",
        "<w:tblPr><w:tblBorders>"
        '<w:top w:val="single" w:sz="4" w:space="0" w:color="999999"/>'
        '<w:left w:val="single" w:sz="4" w:space="0" w:color="999999"/>'
        '<w:bottom w:val="single" w:sz="4" w:space="0" w:color="999999"/>'
        '<w:right w:val="single" w:sz="4" w:space="0" w:color="999999"/>'
        '<w:insideH w:val="single" w:sz="4" w:space="0" w:color="999999"/>'
        '<w:insideV w:val="single" w:sz="4" w:space="0" w:color="999999"/>'
        "</w:tblBorders></w:tblPr>",
    ]
    xml.append("<w:tr>" + "".join(cell(header, bold=True) for header in headers) + "</w:tr>")
    for row in rows:
        xml.append("<w:tr>" + "".join(cell(value) for value in row) + "</w:tr>")
    xml.append("</w:tbl>")
    xml.append(paragraph("", after=120))
    return "".join(xml)


def document_body() -> str:
    parts: list[str] = []
    parts.append(heading("Звіт до лабораторної роботи №3", 1))
    parts.append(paragraph("Тема: Міграція бази даних", bold=True, size=28))
    parts.append(paragraph("Виконавець: номер у списку 22"))
    parts.append(paragraph("Датасет: GlobalWeatherRepository.csv"))
    parts.append(paragraph("Технології: Python, SQLAlchemy ORM, Alembic, PostgreSQL/MySQL"))

    parts.append(heading("1. Мета роботи", 1))
    parts.append(
        paragraph(
            "Розробити застосунок у стилі Layered Architecture, описати структуру "
            "погодної бази через ORM, реалізувати імпорт CSV, міграції схеми без "
            "втрати даних і консольний пошук погоди за країною та датою."
        )
    )

    parts.append(heading("2. Визначення варіанта", 1))
    parts.append(paragraph("Номер у списку: 22."))
    parts.append(paragraph("Категорія визначається за залишком від ділення номера на 4."))
    parts.append(code_block("22 mod 4 = 2"))
    parts.append(paragraph("Отже, для роботи обрано категорію 2: стан повітря / air quality."))

    parts.append(heading("3. Відібрані колонки", 1))
    parts.append(
        table(
            ["Вимога", "Колонка", "Тип у моделі"],
            [
                ["Текстова", "country", "String"],
                ["Ціле число", "wind_degree", "Integer"],
                ["Дробне число", "wind_kph", "Float"],
                ["Enumeration", "wind_direction", "Enum WindDirection"],
                ["Дата", "last_updated", "Date"],
                ["Час", "sunrise", "Time"],
            ],
        )
    )
    parts.append(paragraph("Колонки категорії стану повітря, винесені в окрему таблицю:"))
    for column in [
        "air_quality_carbon_monoxide",
        "air_quality_ozone",
        "air_quality_nitrogen_dioxide",
        "air_quality_sulphur_dioxide",
        "air_quality_pm25",
        "air_quality_pm10",
        "air_quality_us_epa_index",
        "air_quality_gb_defra_index",
    ]:
        parts.append(bullet(column))

    parts.append(heading("4. Архітектура рішення", 1))
    parts.append(bullet("app/domain - сутності, enum, доменна політика оцінки якості повітря."))
    parts.append(bullet("app/application - use-case сервіси імпорту CSV та пошуку погоди."))
    parts.append(bullet("app/infrastructure - CSV reader, SQLAlchemy ORM, репозиторій і конфігурація БД."))
    parts.append(bullet("app/presentation/cli - консольний інтерфейс для користувача."))
    parts.append(bullet("alembic/versions - міграції бази даних."))

    parts.append(heading("5. ORM-модель", 1))
    parts.append(
        paragraph(
            "Основна таблиця WeatherRecord зберігає країну, локацію, дату, час, "
            "температуру, стан погоди, вітер, вологість і час сходу сонця. "
            "Після рефакторингу таблиця AirQualityMetric зберігає всі показники "
            "стану повітря та пов'язана з WeatherRecord через foreign key."
        )
    )
    parts.append(
        paragraph(
            "Напрям вітру описаний enum WindDirection: N, NNE, NE, ENE, E, ESE, "
            "SE, SSE, S, SSW, SW, WSW, W, WNW, NW, NNW."
        )
    )

    parts.append(heading("6. Хід виконання роботи", 1))
    parts.append(heading("6.1. Підготовка проєкту", 2))
    parts.append(code_block("pip install -r requirements.txt"))
    parts.append(paragraph("Було встановлено SQLAlchemy, Alembic, psycopg і PyMySQL."))

    parts.append(heading("6.2. Початкова міграція", 2))
    parts.append(
        paragraph(
            "Міграція 0001_initial_weather_schema створює таблицю weather_records. "
            "На цьому етапі показники стану повітря ще знаходяться в тій самій таблиці."
        )
    )
    parts.append(code_block("python -m alembic upgrade 0001_initial_weather_schema"))

    parts.append(heading("6.3. Імпорт CSV у початкову схему", 2))
    parts.append(code_block("python main.py import-csv --limit 20"))
    parts.append(code_block("schema_stage=legacy\ninserted=20\nskipped=0"))

    parts.append(heading("6.4. Рефакторинг через міграцію", 2))
    parts.append(
        paragraph(
            "Міграція 0002_split_air_quality створює таблицю air_quality_metrics, "
            "переносить туди всі air-quality поля, обчислює is_outdoor_safe і видаляє "
            "ці поля зі старої таблиці weather_records."
        )
    )
    parts.append(code_block("python -m alembic upgrade head"))
    parts.append(code_block("Running upgrade 0001_initial_weather_schema -> 0002_split_air_quality"))

    parts.append(heading("6.5. Перевірка нормалізованої схеми", 2))
    parts.append(code_block("python main.py schema-stage"))
    parts.append(code_block("normalized"))

    parts.append(heading("6.6. Приклад пошуку погоди", 2))
    parts.append(code_block("python main.py find-weather --country Afghanistan --date 2024-05-16"))
    parts.append(
        code_block(
            "Afghanistan | Kabul\n"
            "  updated: 2024-05-16 13:15\n"
            "  timezone: Asia/Kabul\n"
            "  weather: 26.6C, Partly Cloudy, humidity=24%\n"
            "  wind: 13.3 kph, 338 deg, NNW\n"
            "  sunrise: 04:50\n"
            "  air quality: CO=277.0, O3=103.0, NO2=1.1, SO2=0.2, PM2.5=8.4, PM10=26.6, EPA=1, DEFRA=1\n"
            "  should_go_outside: no"
        )
    )

    parts.append(heading("7. Формула поля is_outdoor_safe", 1))
    parts.append(
        paragraph(
            "Поле is_outdoor_safe має значення true лише тоді, коли всі показники "
            "стану повітря не перевищують задані пороги."
        )
    )
    parts.append(code_block(
        "pm25 <= 35\n"
        "pm10 <= 50\n"
        "us_epa_index <= 3\n"
        "gb_defra_index <= 3\n"
        "carbon_monoxide <= 4400\n"
        "ozone <= 100\n"
        "nitrogen_dioxide <= 40\n"
        "sulphur_dioxide <= 20"
    ))
    parts.append(
        paragraph(
            "Якщо хоча б один показник гірший за порогове значення, відповідь "
            "на питання 'Чи варто виходити на вулицю?' буде 'ні'."
        )
    )

    parts.append(heading("8. Запуск з локальним PostgreSQL", 1))
    parts.append(
        paragraph(
            "Docker не є обов'язковим. Якщо PostgreSQL вже встановлений і запущений, "
            "достатньо створити роль і базу через підготовлений скрипт."
        )
    )
    parts.append(code_block(
        "python scripts/setup_postgres.py\n"
        "$env:DATABASE_URL = \"postgresql+psycopg://weather:weather@localhost:5432/weather_lab\"\n"
        "python -m alembic upgrade head\n"
        "python main.py import-csv\n"
        "python main.py find-weather --country Afghanistan --date 2024-05-16"
    ))

    parts.append(heading("9. Міграція між PostgreSQL і MySQL", 1))
    parts.append(
        paragraph(
            "Alembic-міграції написані через SQLAlchemy, тому той самий набір "
            "міграцій можна накотити на PostgreSQL і MySQL. Для переходу змінюється "
            "лише DATABASE_URL."
        )
    )
    parts.append(code_block(
        "$env:DATABASE_URL = \"postgresql+psycopg://weather:weather@localhost:5432/weather_lab\"\n"
        "python -m alembic upgrade head\n\n"
        "$env:DATABASE_URL = \"mysql+pymysql://weather:weather@localhost:3306/weather_lab\"\n"
        "python -m alembic upgrade head"
    ))

    parts.append(heading("10. Відповіді на питання до роботи", 1))
    parts.append(heading("10.1. Чи можна завантажувати всю базу, а описувати в ORM тільки деякі колонки?", 2))
    parts.append(
        paragraph(
            "Так, можна. CSV може містити більше полів, ніж потрібно застосунку. "
            "Під час імпорту програма читає рядок повністю, але в ORM-моделях і "
            "таблицях зберігає лише вибрані колонки. Якщо в таблиці фізично є "
            "додаткові колонки, ORM також може описувати не всі з них."
        )
    )

    parts.append(heading("10.2. Чи є дефолтне значення у колонці is_outdoor_safe?", 2))
    parts.append(
        paragraph(
            "Ні. Дефолтне значення спеціально не задавалось, бо is_outdoor_safe "
            "є похідним полем. Воно має обчислюватися за формулою під час імпорту "
            "або міграції. Дефолт false міг би приховати помилки розрахунку."
        )
    )

    parts.append(heading("10.3. У скільки етапів зроблено третє завдання?", 2))
    parts.append(
        paragraph(
            "У два етапи: спочатку в міграції створюється нова таблиця "
            "air_quality_metrics із колонкою is_outdoor_safe, потім історичні "
            "дані переносяться зі старої таблиці та для кожного рядка обчислюється "
            "значення цього поля."
        )
    )

    parts.append(heading("10.4. На яку частину застосунку покладено заповнення is_outdoor_safe?", 2))
    parts.append(
        paragraph(
            "У прикладному коді це робить доменний сервіс "
            "app/domain/services/air_quality_policy.py. У міграції така сама логіка "
            "продубльована як історичний знімок правила, щоб міграція не залежала "
            "від майбутніх змін у коді застосунку."
        )
    )

    parts.append(heading("10.5. Чи легко було накочувати міграції і переходити з однієї БД на іншу?", 2))
    parts.append(
        paragraph(
            "Загалом так, бо SQLAlchemy і Alembic абстрагують частину відмінностей "
            "між СУБД. Потенційні проблеми: різне представлення enum, boolean, "
            "автоінкременту та ALTER TABLE у PostgreSQL і MySQL. У роботі це "
            "мінімізовано явними типами колонок і міграціями, які переносять дані "
            "перед видаленням старих колонок."
        )
    )

    parts.append(heading("11. Висновок", 1))
    parts.append(
        paragraph(
            "У роботі реалізовано імпорт погодного CSV, ORM-модель, двоетапну "
            "міграцію з нормалізацією таблиць, окрему таблицю стану повітря, "
            "обчислюване поле is_outdoor_safe та консольний пошук погоди за "
            "країною і датою. Міграції дозволяють як створити базу з нуля, так і "
            "перенести вже імпортовані дані без втрати."
        )
    )
    return "".join(parts)


def make_document_xml() -> str:
    body = document_body()
    return f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
  <w:body>
    {body}
    <w:sectPr>
      <w:pgSz w:w="11906" w:h="16838"/>
      <w:pgMar w:top="1134" w:right="1134" w:bottom="1134" w:left="1134" w:header="708" w:footer="708" w:gutter="0"/>
    </w:sectPr>
  </w:body>
</w:document>
"""


CONTENT_TYPES = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>
</Types>
"""

RELS = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/>
</Relationships>
"""


def main() -> int:
    with ZipFile(OUTPUT, "w", compression=ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", CONTENT_TYPES)
        archive.writestr("_rels/.rels", RELS)
        archive.writestr("word/document.xml", make_document_xml())
    print(OUTPUT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
