# Запуск проекта

Переименовать .env.sample -> .env и заменить внутри него авторизационные данные для телеграм на свои

```
docker compose up --build -d
```

## Порты

```
127.0.0.1:8000 - fastapi app
```

`127.0.0.1:8000/docs - swagger doc` - api для отладки

```
127.0.0.1:8080 - airflow dashboard
```

## Логи

Логи по каждому Дагу записываются автоматически в таблицу task_logs базы данных airflow. Для просмотра логов достаточго запросить данные из той таблицы
Либо перейти по ЭПу 127.0.0.1:8000/docs и вызвать /logs (можно не вводя аргументы)
![1761722783186](images/README/1761722783186.png)


## Повестка встречи NLP project 03.11.2025


Содержание

1. Архитектура ПО
2. Флоу разработки

## Архитектура ПО

1. DAG - основное понятие

   1. Dag - это модель, которая включает в себя всё необходимое для выполнения рабочего процесса.
   2. Задача - в рамках DAG - единица работы, которая выполняется в процессе выполнения дага
   3. Расписание - когда должен запускаться рабочий процесс

      Ссылка на документацию https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html
2. Параметры конфигурации .env
   Файл  .env представляет собой хранилище переменных окружения. Он хранит в себе записи о константах, которые могут меняться в процессе развития приложения, например, ссылка на соединение с базой данных или данные для авторизации в телеграмм api
3. Креденталы для airflow , postgres, grafana
   Эти записи можно найти в docker-compose.yaml, но вынесу их сюда

   Grafana: `admin : admin`
   Airflow: `admin : admin`
   Postgre: `airflow : airflow`
4. docker - compose. Как поднимать, удалять как находить в браузере
   Для запуска ПО нужно:
   1. Перейти в папку проекта
   `cd tradeSys`
   2. Прописать команду
   `docker compose up --build -d`
   Логи можно просмотраивать отдельно
   `docker compose logs -f`
   3. Для полного перезапуска контейнеров с очисткой БД
   `docler compose down -v`
   `docker compose up --build -d`
   4. Как найти в браузере запущенные приложения
   swagger fastapi: `127.0.0.1:8000`
   airflow: `127.0.0.1:8080`
   grafana `127.0.0.1:3000`
   posrgres (через pgAdmin или DBever) `postgresql+psycopg2://airflow:airflow@postgres-data:5432/airflow_data`
5. Философия хранения, записи и удаления данных
   **Все данные хранятся в БД**

   1. Если нужно запросить данные - для них нужно создать отдельную таблицу в БД и сначала записать данные в эту таблицу после запроса. Обработка данных должна происходить в отдельной задаче - это создает атомарность процессов для мобильности.
   2. Перед записью данных в таблицу нужно сначала стирать ВСЕ старые данные из этой таблицы (таблица должна полностью перезаписаться) - исключение - последователньая запись данных (типа лооггирование, сводки новостей)
   3. Если нужно сделать обработку и анализ данных - нужно создать отдельную таблицу для обработанных данных, исходные данные должны быть сохранены в своей таблице
   4. Если DAG-у нужны данные - их нужно запросить из БД, если нужно сохранить данные после преобразований - их нужно сохранять в БД

## Флоу разработки

1. Написание своего DAG-a
   Написание задачи для DAG не отличается по сути от написания обычной программы, но нужно действовать в рамках одного шаблона

   ```
   from airflow import DAG
   from airflow.operators.python import PythonOperator
   from datetime import datetime, timedelta
   from sqlalchemy import create_engine
   from plugins.sql_logger import log_task_message
   import osDB_URL_DEFAULT = "postgresql+psycopg2://airflow:airflow@postgres-data:5432/airflow_data"
   DB_URL = os.getenv("DB_URL", DB_URL_DEFAULT)log_task_conf = {
       "dag_id": "< имя вашего DAGa >",
       "task_id": "< имя вашей задачи>",
   }




   == ЗДЕСЬ ВАШ КОД /==



   default_args = {
       "owner": "airflow",
       "retries": 1,
       "retry_delay": timedelta(minutes=10),
   }def log_task_start(context):
       log_task_message(
           dag_id=context["dag"].dag_id,
           task_id=context["task"].task_id,
           log_level="INFO",
           message="Task started",
       )
   with DAG(
       dag_id=log_task_conf["dag_id"],
       default_args=default_args,
       schedule_interval="@daily",
       start_date=datetime(2025, 10, 1),
       catchup=False,
       tags=["analytics", "ohlcv"],) as dag:
       analyze_data = PythonOperator(
           task_id=log_task_conf["task_id"],
           python_callable= <здесь имя функции, выполняющей вашу задачу>,
           on_execute_callback=log_task_start,
       )
   ```
   2. Добавление таблицы в базу данных
      Чтобы добавить новую таблицу в БД, нужно в корне проекта перейти в папку
      `sql` и внести изменения в `init.sql`, по образцу уже созданных там таблиц
      После, выполнить
      ` docker compose down -v`
      `docker compose up --build -d`
      В таком случае контейнеры пересоберутся и в БД появится новая таблица
   3. Отладка - логгирование и просмотр логов
      Логи выполнения задач можно увидеть в двух местах

      1. Grafana - панель логов на дашборде
      2. В базе данных, по запросу таблицы `task_logs`
         Для записи своих логов в таблицу нужно воспользоваться функцией **log_task_message**
   4. Создание своей ветки на git и merge request в main
      Для создания новой ветки в git

      1. Склонировать себе репозиторий
      2. `git branch <имя вашей ветки>`
      3. `git checkout <имя вашей ветки>`
         Для слияния изменений в main ветку проекта
      4. `git add .`
      5. `git commit -m "имя коммита"`
      6. `git push origin <имя вашей ветки>`
         Перейти в интерфейс github, находите репозиторий проекта и нажимаете New pull request, затем выбираете из какой ветки в какую сливать
