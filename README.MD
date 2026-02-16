# MOEX Airflow DAGs

DAG для сбора данных с MOEX и переноса в ClickHouse.

## Описание
- moex_data_loader Даг для Сбор данных по акциям (SBER, GAZP, LKOH и др.) с Сохранение в PostgreSQL и Перенос в ClickHouse через внешнюю таблицу
- update_click_view обновляет после успешного moex_data_loader moex_prices_view с последними ценами, удаляя старые. Даг ждет события через ДатаСет, иначе не получилось, чтобы срабатывал и по ручному запуску и по рассписанию, через ExternalTaskSensor, а через TriggerDagRunOperator не хотелось

## Connections
- `pg_app` - PostgreSQL (postgres:5432, airflow/airflow)
- `clickhouse_default` - ClickHouse (clickhouse:8123)

## Docker
Файл `docker-compose.yml` находится выше по иерархии и подключен через хардлинк.  
Репозиторий клонируется: `git clone https://github.com/Mdewayne/razdupl.git`

## Планы доработки
- [V] Jinja-обработка временных строк в запросах
- [V ] DAG по триггеру MOEX с TRUNCATE + INSERT новой таблицы и заменой на последний данные в витрине
- [ ] Обсудить и исправить логику разбиения по функциям и файлам, и как работать со временем и Jinja правильно.