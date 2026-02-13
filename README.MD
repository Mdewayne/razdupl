# MOEX Airflow DAGs

DAG для сбора данных с MOEX и переноса в ClickHouse.

## Описание
- Сбор данных по акциям (SBER, GAZP, LKOH и др.)
- Сохранение в PostgreSQL
- Перенос в ClickHouse через внешнюю таблицу

## Connections
- `pg_app` - PostgreSQL (postgres:5432, airflow/airflow)
- `clickhouse_default` - ClickHouse (clickhouse:8123)

## Docker
Файл `docker-compose.yml` находится выше по иерархии и подключен через хардлинк.  
Репозиторий клонируется: `git clone https://github.com/Mdewayne/razdupl.git`

## Планы доработки
- [ ] Jinja-обработка временных строк в запросах
- [ ] DAG по триггеру MOEX с TRUNCATE + INSERT новой таблицы и заменой на последний данные в витрине