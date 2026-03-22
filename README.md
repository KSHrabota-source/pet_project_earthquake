# pet_project_earthquake
pet_project_earthquake


``` python3.11 -m venv .venv```

✅Поднял в режиме виртуального окружения .venv Питон 3.11.  
✅pip 26.0.1 — обновил до новейшей версии с PyPI (март 2026)  
✅Установил Airflow провайдеры в .venv (не основной Airflow, т.к. Docker):

```pip install apache-airflow-providers-postgres apache-airflow-providers-minio```

⚠️🛠️ провайдеры = мост между локальной разработкой (.venv) и production оркестрацией (Docker Airflow), содержащий специализированные классы для интеграции с postgres/minio.


✅ Поднял PostgreSQL + MinIO через Docker Compose (готовлю инфраструктуру для Airflow), файл с Контейнером инсраструктуры Айрфлоу скачал на официальном сайте Айрфлоу.

✅ Добавил в контейнерную среду Пайтон и Минио.

⚠️🛠️Минио - это высокопроизводительное объектное хранилище с открытым исходным кодом, полностью совместимое с Amazon S3 API. Оно создано для хранения неструктурированных данных: изображения, видео, логи, бэкапы, датасеты ML — всё, что не помещается в реляционные БД.

✅ Добавил Metabase — инструмент для создания дашбордов и визуализации данных прямо из PostgreSQL!

    ⚠️🛠️Этот сервис:
	•	Подключается к postgres_dwh:5432 (ваша база землетрясений)
	•	Создаёт интерактивные графики: карта эпицентров, тренды магнитуд, статистика по глубине
	•	Drag&Drop дашборды без кода
✅Вручную создал папку Metabas, создал ДокерФайл и написал в него: 
FROM metabase/metabase:latest - 'возьми готовое из докер Хаб'

✅Поднял контейнер:

    ✅ Скачались образы (postgres, minio, metabase)
    ✅ Созданы контейнеры (3 штуки)
    ✅ Запущены сервисы в фоне
    ✅ Открыты порты 5432, 9000-9001, 3000

✅Установил DuckDB в .venv** — SQL расчёты на CSV файлах. DuckDB это СУБД  для аналитических запросов, функционирует как иблиотека, в отличии PostgreSQL имеет колоночный формат. Удобен тем что считает быстро, максимально загружает процессор для расчетов, может работать с данными сразу в облачном хранилище без скачинвания данных, распараллеливает процессы при расчетах по ядрам.
 Почитать о ней можно по ссылке https://bigdataschool.ru/wiki/duckdb/ **Пример:** USGS землетрясения.csv → "Сколько сильных quakes > 5 магнитуд?"  
⚠️🛠️ файлы CSV скачиваются в виде текста с нашего сайт с землятрясениями

✅ Создал Пайтон файл cret.py = Python файл = РОБОТ, который далее будет делать следующие шаги:
1. Скачивает CSV с землетрясениями (Extract)
2. Считает статистику (Transform) 
3. Загружает в PostgreSQL + MinIO (Load)

                    ## Разбор кода DAG

```import logging```  
⚠️🛠️ import это подключение библиотек.  
⚠️🛠️ импортируем инструмент для записи логов в Airflow UI, их еще нет, позже напишу.  
```  import duckdb```   
⚠️🛠️ запускает SQL калькулятор для ускорения расчетов, читает CSV из интернета → считает магнитуды → сохраняет в MinIO.  
```import pendulum```.  
⚠️🛠️ запускает библиотеку Пайтон для Айрфлоу, часы для дат и часовых поясов (Москва)  
```from airflow import DAG```.  
⚠️🛠️ запускает DAG из Айфлоу  
```from airflow.models import Variable```  
⚠️🛠️ Импортирует класс Variable из Airflow. Позволяет достать `access_key` и `secret_key` из UI. НЕ хранит пароли прямо в коде (безопасно!)  
```from airflow.operators.empty import EmptyOperator```.  
⚠️🛠️ подключаю в код оператор EmptyOperator из модуля Airflow, который создаёт «пустую» задачу, которая ничего не делает  
```from airflow.operators.python import PythonOperator```.   
⚠️🛠️ из Айрфлоу из модуля Пайтон подключаю класс Операторов пайтон.   
```OWNER = "konstantinshevtsov"```.  
⚠️🛠️ параметр DAG Airflow UI имя создателя  
```DAG_ID = "earthquake_etl"```  
⚠️🛠️ параметр DAG, его имя   
```LAYER = "raw"```  
⚠️🛠️ слой данных для MinIO (S3 хранилища). LAYER = адрес папки в MinIO (куда сохранить): raw = сырые CSV из USGS API:
   - DuckDB читает CSV из интернета
   - LAYER = "raw" говорит: "сохрани в папку raw/"
   - MinIO получает: s3://prod/raw/earthquake/2026-03-14/data.parquet  

```SOURCE = "earthquake"```  
⚠️🛠️ константа SOURCE = "какой источник данных?" - "earthquake" = данные о землетрясениях USGS  
⚠️🛠️ SOURCE используется в SQL запросе DuckDB  
```ACCESS_KEY = Variable.get("access_key")```  
⚠️🛠️ константа для логина для MinIO, сам логин хранится в файле Cret.py достается из Airflow UI  
```SECRET_KEY = Variable.get("secret_key")```.  
⚠️🛠️ константа для пароля для MinIO, сам логин хранится в файле Cret.py достается из Airflow UI  

```LONG_DESCRIPTION = """
- Скачивает данные каждый день в 00:00 UTC
- Сохраняет в MinIO как Parquet файлы
- Bucket: earthquakes/raw
"""
```  
⚠️🛠️ длинное описание DAG для Airflow UI  
⚠️🛠️ Airflow читает файл → видит LONG_DESCRIPTION   
```SHORT_DESCRIPTION = "Ежедневная загрузка землетрясений USGS в MinIO"```  
⚠️🛠️ короткое описание DAG для Airflow UI, появляется рядом с именем DAG'а в списке
```args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 3, 10, tz="Europe/Moscow"),
    "catchup": False,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}
```  
⚠️🛠️ Создаём словарь настроек. Это настройки для всех файлов в DAG. Здесь создается словарь с параметрами по умолчанию и передается в DAG через этот аргумент args  
⚠️🛠️ 'owner': 'OWNER'     → кто отвечает за DAG (я)  
⚠️🛠️ "start_date": pendulum.datetime(2025, 5, 1, tz="Europe/Moscow") → определяет когда DAG начал работать через библиотеку дат в пайтон с часовыми поясами  
⚠️🛠️ "catchup": False, определяет запуск с указанной даты с учетом пропущенных дней или нет  
⚠️🛠️ 'retries': 1  → опредляет что будет если упадёт программа → программа попробует 1 раз перезапуститься  
⚠️🛠️ retry_delay': timedelta(minutes=5) → пауза после падения 5 минут перед повтором retries  

```def get_dates(**context) -> tuple[str, str]:
    """Возвращает start_date и end_date из контекста Airflow"""
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")

    return start_date, end_date
```

⚠️🛠️ создаем пайтон функцию, которая узнаёт ДАТЫ запуска DAG  
⚠️🛠️ get_dates имя функции в нее передается.  
⚠️🛠️ -> tuple[str, str] = функция ВЕРНЁТ две строки (start и end даты)  
⚠️🛠️     start_date = context["data_interval_start"].format("YYYY-MM-DD") именуем переменную и в контекст передается  начало работы интервала и сразу форматируется в строку этот текст по заданному формату.  
⚠️🛠️ end_date = context["data_interval_end"].format("YYYY-MM-DD") именуем переменную и в контекст передается  конец работы интервала и сразу форматируется в строку этот текст по заданному формату   
⚠️🛠️ **context    = Airflow передаёт СЮДА всю информацию о запуске: начало и конец интервала работы в виде списка из строк через:  
⚠️🛠️ return start_date, end_dateВозвращаем ОБЕ даты обратно тому кто вызвал функцию: → ("2026-03-14", "2026-03-15")  

```def get_and_transfer_api_data_to_s3(**context): 
    """Скачивает CSV с USGS API и сохраняет в MinIO как Parquet"""

    start_date, end_date = get_dates(**context)
    logging.info(f"💻 Начало загрузки дат: {start_date}/{end_date}")
    con = duckdb.connect()
```

⚠️🛠️ создаем главную функцию ДАГ с командой на получение и перенос данных в S3 передавая все в многострочный контекст. 
⚠️🛠️ вызываем Функцию get_dates и принимаем переменные начала и   конца интервала работы из нее, передаем все в контекст
⚠️🛠️ пишем в Лог сообщение с подстановкой переменных начала и конца интервала работы  
⚠️🛠️ через con вызываем калькулятор SQL ДакДБ. Это метод запуска ДакДБ,  con  = сокращение от “connection”   

```con.sql(
        f"""
        SET TIMEZONE='UTC';
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = '{ACCESS_KEY}';
        SET s3_secret_access_key = '{SECRET_KEY}';
        SET s3_use_ssl = FALSE;

        COPY
        (
            SELECT
                *
            FROM
                read_csv_auto('https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&starttime={start_date}&endtime={end_date}') AS res
        ) TO 's3://prod/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet';

        """,
    )
```


⚠️🛠️ пишем SQL запрос через con - это = ГЛАВНЫЙ SQL запрос — качает данные и сохраняет в MinIO.  
⚠️🛠️ строку кода с подстановкой переменных через f"""""".  
⚠️🛠️ указываем для Дак ДБ часовой пояс,  скачиваем расширение для работы с интернетом, подключаем его, устанавливаем тип локального подключения.  
⚠️🛠️ path, объявляем порт для подключения Минио 9000.  
⚠️🛠️ указываем логин и пароль для Минио через переменные, которые есть в докер компоусе.  
⚠️🛠️ указываем что шифрование соединения не нужно, так как MinIO локальный внутри Docker.  
⚠️🛠️ read_csv_auto(...)  — функция DuckDB, которая читает CSV по URL и сама определяет разделители, типы колонок.  
⚠️🛠️ селектим всё за указанные ранее даты интервала из ссылки в интернете с запуском функции автоматического чтения CSV.  
⚠️🛠️ сохраняем результат в файл в S3/MinIO, путь сохранения создается из переменных, которые мы создали выше.  

```con.close()
    logging.info(f"✅ Download for date success: {start_date}")
```

⚠️🛠️ закрыли соединение (коннекшн) и написали лог с выводом переменной даты через пайтон функию f.  

```with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 5 * * *",
    default_args=args,
    tags=["s3", "raw"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
```

⚠️🛠️ создаем даг и прописываем его параметры, Айди, интервал запуска каждый день в 5 утра, теги, короткое описание из переменной выше количество   
⚠️🛠️ одновременно запущенных задач 1 (без паралелльных задач). 

```
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(
        task_id="start",
    )

    get_and_transfer_api_data_to_s3 = PythonOperator(
        task_id="get_and_transfer_api_data_to_s3",
        python_callable=get_and_transfer_api_data_to_s3,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> get_and_transfer_api_data_to_s3 >> end
```

⚠️🛠️пропишем Маркдовн файл для поля ДагсДетали в АйфлоуЮИ там
будет описание дага, эмпти задачи пустышки на старте и финише

⚠️🛠️python_callable=get_and_transfer_api_data_to_s3  — говорит Airflow: “запусти вот эту Python-функцию”, ту самую которую мы разбирали выше.  
⚠️🛠️ в конце описание зависимостей для определения порядка дага - наш направленный не цикличный граф  

                ## РАЗБОР КОДА ОКОНЧЕН


                ## ОПИСЫВАЮ ДАГ 2

***

## Импорты и базовые настройки

```python
import logging
```
⚠️🛠️ Подключаем стандартную библиотеку Python для логирования.  
⚠️🛠️ Через неё пишем сообщения, которые потом видны в логах задач в Airflow UI.

```python
import duckdb
```
⚠️🛠️ Импортируем DuckDB — встраиваемую аналитическую БД.  
⚠️🛠️ В этом DAG DuckDB читает Parquet из MinIO и записывает данные в PostgreSQL через своё расширение postgres.[1][2]

```python
import pendulum
```
⚠️🛠️ Импортируем библиотеку для работы с датами и часовыми поясами.  
⚠️🛠️ Airflow рекомендует Pendulum: им задаём `start_date` и интервалы в нужной таймзоне (Москва).

```python
from airflow import DAG
```
⚠️🛠️ Импортируем класс DAG — «каркас» нашего пайплайна в Airflow.  
⚠️🛠️ Через него создаём сам граф задач (Directed Acyclic Graph).

```python
from airflow.models import Variable
```
⚠️🛠️ Импортируем класс `Variable`, чтобы доставать значения из Airflow UI (логины/пароли).  
⚠️🛠️ Это позволяет не хранить секреты (ключи, пароли) прямо в коде.

```python
from airflow.operators.empty import EmptyOperator
```
⚠️🛠️ Импортируем `EmptyOperator` — «пустую задачу», которая ничего не делает.  
⚠️🛠️ Используем как технические точки «start» и «end» для красоты графа и удобства зависимостей.

```python
from airflow.operators.python import PythonOperator
```
⚠️🛠️ Импортируем оператор, который умеет запускать Python‑функции как задачи Airflow.  
⚠️🛠️ Мы привяжем к нему функцию `fetch_and_transfer_raw_data_to_ods_pg`.

```python
from airflow.sensors.external_task import ExternalTaskSensor
```
⚠️🛠️ Импортируем сенсор, который ждёт завершения задачи в другом DAG.  
⚠️🛠️ Нужен, чтобы не начинать загрузку в Postgres, пока не закончен DAG, который кладёт данные в S3.

***

## Константы DAG и слои данных

```python
OWNER = "konstantinshevtsov"
DAG_ID = "raw_from_s3_to_pg"
```
⚠️🛠️ `OWNER` — логическое имя владельца DAG, показывается в UI и в логах.  
⚠️🛠️ `DAG_ID` — уникальный идентификатор DAG в Airflow, по нему его видим в списке DAG’ов.

```python
LAYER = "raw"
SOURCE = "earthquake"
SCHEMA = "ods"
TARGET_TABLE = "fct_earthquake"
```
⚠️🛠️ `LAYER` — слой данных в хранилище (raw/stg/ods/dm). Здесь `raw` — сырые данные в S3/MinIO.  
⚠️🛠️ `SOURCE` — название источника, используется в пути к файлам: `earthquake` = данные USGS о землетрясениях.  
⚠️🛠️ `SCHEMA` — схема в Postgres, куда будем писать (`ods`).  
⚠️🛠️ `TARGET_TABLE` — имя целевой таблицы в этой схеме (`fct_earthquake`).

```python
LONG_DESCRIPTION = """
# LONG DESCRIPTION
"""
SHORT_DESCRIPTION = "SHORT DESCRIPTION"
```
⚠️🛠️ Описания для DAG: длинное (Markdown) и короткое.  
⚠️🛠️ Короткое показывается в списке DAG’ов, длинное — в подробном описании DAG.

***

## Параметры по умолчанию для задач

```python
args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 3, 10, tz="Europe/Moscow"),
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}
```
⚠️🛠️ Словарь `args` — `default_args` для всех задач в DAG.  
⚠️🛠️ `owner` — владелец задач (подтягиваем из константы).  
⚠️🛠️ `start_date` — с какого момента DAG «логически» начинает работать; влияет на интервалы `data_interval_start/end`.  
⚠️🛠️ `retries` — сколько раз Airflow попробует перезапустить задачу при ошибке (3 раза).  
⚠️🛠️ `retry_delay` — пауза между попытками (1 час).

***

## Вспомогательная функция дат

```python
def get_dates(**context) -> tuple[str, str]:
    """"""
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")

    return start_date, end_date
```
⚠️🛠️ Объявляем функцию, которая вытаскивает из контекста Airflow две даты: начало и конец интервала данных.  
⚠️🛠️ `**context` — Airflow передаёт сюда служебную информацию о запуске (execution_date, data_interval и т.д.).  
⚠️🛠️ `data_interval_start` / `data_interval_end` → форматируем в строки `YYYY-MM-DD`, чтобы подставить в пути/URL.  
⚠️🛠️ Возвращаем кортеж из двух строк: `(start_date, end_date)`.

***

## Главная функция: из S3 (MinIO) в Postgres

```python
def fetch_and_transfer_raw_data_to_ods_pg(**context):
    """"""
```
⚠️🛠️ Основная Python‑функция таски: читает файл из S3 и записывает в Postgres.  
⚠️🛠️ Имя функции говорит: «забери raw‑данные и перенеси их в ODS в Postgres».

```python
    access_key = Variable.get("access_key").replace("'", "''")
    secret_key = Variable.get("secret_key").replace("'", "''")
    password = Variable.get("pg_password").replace("'", "''")
```
⚠️🛠️ Читаем значения из Airflow Variables: ключ/секрет для MinIO и пароль Postgres.  
⚠️🛠️ `replace("'", "''")` — экранируем одинарные кавычки, чтобы не сломать SQL‑строку (внутри `f""" ... '{password}' ... """`).

```python
    start_date, end_date = get_dates(**context)
    logging.info(f"Start load for dates: {start_date}/{end_date}")
    con = duckdb.connect()
```
⚠️🛠️ Вызываем `get_dates` и получаем интервал данных за текущий запуск DAG.  
⚠️🛠️ Пишем информационный лог — удобно видеть, за какие даты сейчас идёт загрузка.  
⚠️🛠️ `duckdb.connect()` — создаём соединение с DuckDB (ин‑memory БД по умолчанию).

***

## Большой SQL‑блок в DuckDB

```python
    try:
        con.sql(
            f"""
            SET TIMEZONE='UTC';
```
⚠️🛠️ Открываем `try`, чтобы гарантированно закрыть соединение в `finally`.  
⚠️🛠️ Через `con.sql` выполняем один большой SQL‑скрипт в DuckDB.  
⚠️🛠️ `SET TIMEZONE='UTC';` — выставляем таймзону для операций времени в DuckDB.

```sql
            INSTALL httpfs;
            LOAD httpfs;
```
⚠️🛠️ `httpfs` — расширение DuckDB, которое умеет ходить в HTTP/S3‑хранилища.[3][4]
⚠️🛠️ `INSTALL` — загрузить расширение (скачать при необходимости), `LOAD` — подключить его в текущую сессию.

```sql
            INSTALL postgres;
            LOAD postgres;
```
⚠️🛠️ Подключаем расширение `postgres` в DuckDB.[2][1]
⚠️🛠️ Оно позволяет «видеть» PostgreSQL как внешнюю БД: читать и писать таблицы, выполнять запросы через DuckDB.

```sql
            SET s3_url_style = 'path';
            SET s3_endpoint = 'minio:9000';
            SET s3_access_key_id = '{access_key}';
            SET s3_secret_access_key = '{secret_key}';
            SET s3_use_ssl = FALSE;
```
⚠️🛠️ Настраиваем работу DuckDB с MinIO (S3‑совместимое хранилище).[4][5][6]
⚠️🛠️ `s3_url_style='path'` и `s3_endpoint='minio:9000'` — говорим: «S3‑сервер внутри Docker по имени `minio` на порту 9000, используем path‑style URL».  
⚠️🛠️ `s3_access_key_id` и `s3_secret_access_key` — логин/пароль для MinIO из Variables.  
⚠️🛠️ `s3_use_ssl = FALSE` — SSL не нужен, всё локально в docker‑сети.

***

## Настройка подключения к Postgres через DuckDB Secrets

```sql
            CREATE SECRET dwh_postgres (
                TYPE postgres,
                HOST 'postgres_dwh',
                PORT 5432,
                DATABASE postgres,
                USER 'postgres',
                PASSWORD '{password}'
            );
```
⚠️🛠️ Создаём секрет в DuckDB с параметрами подключения к Postgres.[1]
⚠️🛠️ `TYPE postgres` — говорим, что это секрет именно для Postgres‑расширения.  
⚠️🛠️ `HOST 'postgres_dwh'` — имя контейнера Postgres внутри Docker‑сети (не `localhost`).  
⚠️🛠️ `DATABASE postgres`, `USER 'postgres'`, `PASSWORD '{password}'` — конкретная БД и пользователь, куда будем писать.

```sql
            ATTACH '' AS dwh_postgres_db (TYPE postgres, SECRET dwh_postgres);
```
⚠️🛠️ Команда `ATTACH` подключает целую базу Postgres к DuckDB как внешний «каталог».[7][2]
⚠️🛠️ Пустая строка `''` в строке подключения означает: «используй данные из секрета `dwh_postgres`».  
⚠️🛠️ Теперь `dwh_postgres_db.ods.fct_earthquake` для DuckDB выглядит как таблица, с которой можно работать.

***

## Гарантируем, что схема и таблица есть

```sql
            CALL postgres_execute('dwh_postgres_db', 'CREATE SCHEMA IF NOT EXISTS ods');
```
⚠️🛠️ `postgres_execute` — функция DuckDB, которая выполняет произвольный SQL **на стороне Postgres**, а не в DuckDB.[2][1]
⚠️🛠️ Здесь она создаёт схему `ods`, если её нет. Если схема уже есть — ничего страшного, `IF NOT EXISTS` не даёт ошибку.

```sql
            CALL postgres_execute('dwh_postgres_db', '
                CREATE TABLE IF NOT EXISTS ods.fct_earthquake (
                    time varchar,
                    latitude varchar,
                    longitude varchar,
                    depth varchar,
                    mag varchar,
                    mag_type varchar,
                    nst varchar,
                    gap varchar,
                    dmin varchar,
                    rms varchar,
                    net varchar,
                    id varchar,
                    updated varchar,
                    place varchar,
                    type varchar,
                    horizontal_error varchar,
                    depth_error varchar,
                    mag_error varchar,
                    mag_nst varchar,
                    status varchar,
                    location_source varchar,
                    mag_source varchar
                )
            ');
```
⚠️🛠️ Ещё один вызов `postgres_execute`: создаёт таблицу `ods.fct_earthquake` в Postgres, если её нет.  
⚠️🛠️ Схема колонок соответствует тем полям, которые мы будем вставлять из Parquet.  
⚠️🛠️ Это решает твою прежнюю проблему: даже после пересоздания контейнера Postgres DAG сам создаст нужную структуру.

***

## Вставка данных из MinIO в Postgres

```sql
            INSERT INTO dwh_postgres_db.{SCHEMA}.{TARGET_TABLE}
            (
                time,
                latitude,
                longitude,
                depth,
                mag,
                mag_type,
                nst,
                gap,
                dmin,
                rms,
                net,
                id,
                updated,
                place,
                type,
                horizontal_error,
                depth_error,
                mag_error,
                mag_nst,
                status,
                location_source,
                mag_source
            )
            SELECT
                time,
                latitude,
                longitude,
                depth,
                mag,
                magType AS mag_type,
                nst,
                gap,
                dmin,
                rms,
                net,
                id,
                updated,
                place,
                type,
                horizontalError AS horizontal_error,
                depthError AS depth_error,
                magError AS mag_error,
                magNst AS mag_nst,
                status,
                locationSource AS location_source,
                magSource AS mag_source
            FROM 's3://prod/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet';
```
⚠️🛠️ `INSERT INTO dwh_postgres_db.{SCHEMA}.{TARGET_TABLE}` — вставляем строки в таблицу Postgres, подключённую как `dwh_postgres_db.ods.fct_earthquake`.  
⚠️🛠️ В `SELECT` читаем Parquet‑файл из MinIO для конкретной даты `start_date`.  
⚠️🛠️ Переименовываем поля с «верблюжьим» стилем из сырых данных (например, `magType`) в snake_case (`mag_type`), чтобы соответствовать колонкам в таблице.  
⚠️🛠️ Таким образом, DuckDB:
- читает Parquet из S3,  
- приводит колонки к нужным именам,  
- отправляет результат в Postgres как обычный `INSERT`.

***

## Завершение функции

```python
    finally:
        con.close()

    logging.info(f"Download for date success: {start_date}")
```
⚠️🛠️ `finally` гарантирует, что соединение с DuckDB будет закрыто даже при ошибках.  
⚠️🛠️ После успешной вставки пишем лог об успешной загрузке за конкретную дату.

***

## Объявление DAG и задач

```python
with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 5 * * *",
    default_args=args,
    catchup=True,
    tags=["s3", "ods", "pg"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
```
⚠️🛠️ Создаём DAG в контекстном менеджере `with`. Всё, что объявлено внутри, относится к этому DAG.  
⚠️🛠️ `schedule_interval="0 5 * * *"` — запуск каждый день в 05:00 (крон‑выражение).  
⚠️🛠️ `catchup=True` — при включении DAG он «догонит» все даты с `start_date`.  
⚠️🛠️ `tags` — метки для группировки DAG’ов в UI (по слоям/технологиям).  
⚠️🛠️ `concurrency`, `max_active_tasks`, `max_active_runs` — ограничения на параллелизм, чтобы не перегружать локальный кластер.

```python
    dag.doc_md = LONG_DESCRIPTION
```
⚠️🛠️ Привязываем длинное Markdown‑описание к DAG, оно будет видно в UI на вкладке Details.

```python
    start = EmptyOperator(
        task_id="start",
    )
```
⚠️🛠️ «Пустая» стартовая задача, для наглядного начала пайплайна и возможных расширений.

```python
    sensor_on_raw_layer = ExternalTaskSensor(
        task_id="sensor_on_raw_layer",
        external_dag_id="raw_from_api_to_s3",
        external_task_id="end",
        allowed_states=["success"],
        mode="reschedule",
        timeout=360000,
        poke_interval=60,
    )
```
⚠️🛠️ Сенсор ждёт завершения **другого DAG** `raw_from_api_to_s3`, его задачи `end`.  
⚠️🛠️ `allowed_states=["success"]` — ждём, пока та задача не станет успешной.  
⚠️🛠️ `mode="reschedule"` — сенсор «усыпляет» себя между проверками, не занимая слот воркера.  
⚠️🛠️ `poke_interval=60` — проверка раз в 60 секунд; `timeout=360000` — сколько максимум можем ждать (в секундах).

```python
    transfer_raw_data_task = PythonOperator(
        task_id="get_and_transfer_raw_data_to_ods_pg",
        python_callable=fetch_and_transfer_raw_data_to_ods_pg,
    )
```
⚠️🛠️ Оператор, запускающий нашу основную функцию `fetch_and_transfer_raw_data_to_ods_pg`.  
⚠️🛠️ `task_id` — имя задачи в DAG; `python_callable` — какая именно функция будет выполнена.

```python
    end = EmptyOperator(
        task_id="end",
    )
```
⚠️🛠️ Завершающая «пустая» задача, удобно видеть чёткое завершение пайплайна.

```python
    start >> sensor_on_raw_layer >> transfer_raw_data_task >> end
```
⚠️🛠️ Описываем порядок задач в DAG (направленный ацикличный граф).  
⚠️🛠️ Сначала `start`, потом ждём сырой слой (`sensor_on_raw_layer`), затем перенос в Postgres (`transfer_raw_data_task`), в конце — `end`.

