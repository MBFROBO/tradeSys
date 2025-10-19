# Запуск проекта

```
docker compose up --build -d
```

```
docker compose run --rm airflow-init airflow db init
docker compose run --rm airflow-init airflow users create \
  --username admin --password admin --firstname Admin --lastname User \
  --role Admin --email admin@example.com
```

## Порты

```
127.0.0.1:8000 - fastapi app
```

```
127.0.0.1:8080 - airflow dashboard
```
