import requests
import logging
from airflow.exceptions import AirflowException
from moex.config import (
    CLICKHOUSE_HOST, 
    CLICKHOUSE_PORT, 
    CLICKHOUSE_DB
)

def execute_clickhouse_sql(sql, database=CLICKHOUSE_DB):
    url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}"
    params = {'database': database}
    
    logging.info(f"Executing on {database}: {sql[:100]}...")
    
    try:
        response = requests.post(
            url, 
            params=params, 
            data=sql.encode('utf-8'),
            timeout=30
        )
        response.raise_for_status()
        
        result = response.text
        if result.strip():
            logging.info(f"Response: {result[:200]}")
        return result
        
    except requests.exceptions.RequestException as e:
        logging.error(f"ClickHouse error: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logging.error(f"Response: {e.response.text}")
        raise AirflowException(f"ClickHouse query failed: {e}")