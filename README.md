# pet_project_earthquake
pet_project_earthquake



``` python3.11 -m venv .venv```

✅Поднял в режиме виртуального окружения .venv Питон 3.11
✅pip 26.0.1 — обновил до новейшей версии с PyPI (март 2026)
✅Установил Airflow провайдеры в .venv (не основной Airflow, т.к. Docker):

```pip install apache-airflow-providers-postgres apache-airflow-providers-minio```

## провайдеры = мост между локальной разработкой (.venv) и production оркестрацией (Docker Airflow), содержащий специализированные классы для интеграции с postgres/minio.


✅ Поднял PostgreSQL + MinIO через Docker Compose (готовлю инфраструктуру для Airflow), файл с Контейнером инсраструктуры Айрфлоу скачал на официальном сайте Айрфлоу.

✅ Добавил в контейнерную среду Пайтон и Минио.

Минио - это высокопроизводительное объектное хранилище с открытым исходным кодом, полностью совместимое с Amazon S3 API. Оно создано для хранения неструктурированных данных: изображения, видео, логи, бэкапы, датасеты ML — всё, что не помещается в реляционные БД.

✅ Добавил Metabase — инструмент для создания дашбордов и визуализации данных прямо из PostgreSQL!

    Этот сервис:
	•	Подключается к postgres_dwh:5432 (ваша база землетрясений)
	•	Создаёт интерактивные графики: карта эпицентров, тренды магнитуд, статистика по глубине
	•	Drag&Drop дашборды без кода
Вручную создал папку Metabas, создал ДокерФайл и написал в него: 
FROM metabase/metabase:latest - 'возьми готовое из докер Хаб'

✅Поднял контейнер:

    ✅ Скачались образы (postgres, minio, metabase)
    ✅ Созданы контейнеры (3 штуки)
    ✅ Запущены сервисы в фоне
    ✅ Открыты порты 5432, 9000-9001, 3000

✅Установил DuckDB в .venv** — SQL расчёты на CSV файлах. DuckDB это СУБД  для аналитических запросов, функционирует как библиотека,
в отличии PostgreSQL имеет колоночный формат. Удобен тем что считает быстро, максимально загружает процессор для расчетов, может работать
с данными сразу в облачном хранилище без скачинвания данных, распараллеливает процессы при расчетах по ядрам.
 почитать о ней можно по ссылке https://bigdataschool.ru/wiki/duckdb/
**Пример:** USGS землетрясения.csv → "Сколько сильных quakes > 5 магнитуд?"
## файлы CSV скачиваются в виде текста с нашего сайт с землятрясениями

✅ Создал Пайтон файл cret.py = Python файл = РОБОТ, который далее будет делать следующие шаги:
1. Скачивает CSV с землетрясениями (Extract)
2. Считает статистику (Transform) 
3. Загружает в PostgreSQL + MinIO (Load)

                                                        Состав файла  DAGS
import logging  
# import это подключение библиотек
## импортируем инструмент для записи логов в Airflow UI, их еще нет, позже напишу
import duckdb
## запускает SQL калькулятор для ускорения расчетов, читает CSV из интернета → считает магнитуды → сохраняет в MinIO
import pendulum
## запускает библиотеку Пайтон для Айрфлоу, часы для дат и часовых поясов (Москва)
from airflow import DAG
## запускает DAG из Айфлоу
from airflow.models import Variable
## Импортирует класс Variable из Airflow. Позволяет достать `access_key` и `secret_key` из UI. НЕ хранит пароли прямо в коде (безопасно!)
from airflow.operators.empty import EmptyOperator
# подключаю в код оператор EmptyOperator из модуля Airflow, который создаёт «пустую» задачу, которая ничего не делает
from airflow.operators.python import PythonOperator
# из Айрфлоу из модуля Пайтон подключаю класс Операторов пайтон
OWNER = "konstantinshevtsov"
## параметр DAG Airflow UI имя создателя
DAG_ID = "earthquake_etl"
## параметр DAG, его имя 
LAYER = "raw"  
## слой данных для MinIO (S3 хранилища). LAYER = адрес папки в MinIO (куда сохранить): raw = сырые CSV из USGS API:
   - DuckDB читает CSV из интернета
   - LAYER = "raw" говорит: "сохрани в папку raw/"
   - MinIO получает: s3://prod/raw/earthquake/2026-03-14/data.parquet
SOURCE = "earthquake"
## константа SOURCE = "какой источник данных?" - "earthquake" = данные о землетрясениях USGS
## SOURCE используется в SQL запросе DuckDB
ACCESS_KEY = Variable.get("access_key")
## константа для логина для MinIO, сам логин хранится в файле Cret.py достается из Airflow UI
SECRET_KEY = Variable.get("secret_key")
## константа для пароля для MinIO, сам логин хранится в файле Cret.py достается из Airflow UI
LONG_DESCRIPTION = """
- Скачивает данные каждый день в 00:00 UTC
- Сохраняет в MinIO как Parquet файлы
- Bucket: earthquakes/raw
""" 
# длинное описание DAG для Airflow UI
# Airflow читает файл → видит LONG_DESCRIPTION
SHORT_DESCRIPTION = "Ежедневная загрузка землетрясений USGS в MinIO"
# короткое описание DAG для Airflow UI, появляется рядом с именем DAG'а в списке
args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 5, 1, tz="Europe/Moscow"),
    "catchup": False,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}
# Создаём словарь настроек. Это настройки для всех файлов в DAG. Здесь создается словарь с параметрами по умолчанию и передается в DAG через этот аргумент args
# 'owner': 'OWNER'     → кто отвечает за DAG (я)
# "start_date": pendulum.datetime(2025, 5, 1, tz="Europe/Moscow") → определяет когда DAG начал работать через библиотеку дат в пайтон с часовыми поясами
# "catchup": False, определяет запуск с указанной даты с учетом пропущенных дней или нет
# 'retries': 1  → опредляет что будет если упадёт программа → программа попробует 1 раз перезапуститься
# retry_delay': timedelta(minutes=5) → пауза после падения 5 минут перед повтором retries

def get_dates(**context) -> tuple[str, str]:
    """Возвращает start_date и end_date из контекста Airflow"""
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")

    return start_date, end_date
# создаем пайтон функцию, которая узнаёт ДАТЫ запуска DAG
# get_dates имя функции в нее передается
## -> tuple[str, str] = функция ВЕРНЁТ две строки (start и end даты)
##     start_date = context["data_interval_start"].format("YYYY-MM-DD") именуем переменную и в контекст передается  начало работы интервала и сразу форматируется в строку этот текст по заданному формату
## end_date = context["data_interval_end"].format("YYYY-MM-DD") именуем переменную и в контекст передается  конец работы интервала и сразу форматируется в строку этот текст по заданному формату
## **context    = Airflow передаёт СЮДА всю информацию о запуске: начало и конец интервала работы в виде списка из строк через:
## return start_date, end_dateВозвращаем ОБЕ даты обратно тому кто вызвал функцию: → ("2026-03-14", "2026-03-15")

def get_and_transfer_api_data_to_s3(**context): 
    """Скачивает CSV с USGS API и сохраняет в MinIO как Parquet"""

    start_date, end_date = get_dates(**context)
    logging.info(f"💻 Начало загрузки дат: {start_date}/{end_date}")
    con = duckdb.connect()

# создаем главную функцию ДАГ с командой на получение и перенос данных в S3 передавая все в многострочный контекст
# вызываем Функцию get_dates и принимаем переменные начала и конца интервала работы из нее, передаем все в контекст
# пишем в Лог сообщение с подстановкой переменных начала и конца интервала работы
# через con вызываем калькулятор SQL ДакДБ. Это метод запуска ДакДБ,  con  = сокращение от “connection” 

con.sql(
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
# пишем SQL запрос через con - это = ГЛАВНЫЙ SQL запрос — качает данные и сохраняет в MinIO
# строку кода с подстановкой переменных через f""""""
# указываем для Дак ДБ часовой пояс,  скачиваем расширение для работы с интернетом, подключаем его, устанавливаем тип локального подключения
# path, объявляем порт для подключения Минио 9000
# указываем логин и пароль для Минио через переменные, которые есть в докер компоусе
# указываем что шифрование соединения не нужно, так как MinIO локальный внутри Docker
# read_csv_auto(...)  — функция DuckDB, которая читает CSV по URL и сама определяет разделители, типы колонок
# селектим всё за указанные ранее даты интервала из ссылки в интернете с запуском функции автоматического чтения CSV
# сохраняем результат в файл в S3/MinIO, путь сохранения создается из переменных, которые мы создали выше

con.close()
    logging.info(f"✅ Download for date success: {start_date}")