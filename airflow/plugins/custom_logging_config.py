import copy
from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG

LOGGING_CONFIG = copy.deepcopy(DEFAULT_LOGGING_CONFIG)

LOGGING_CONFIG['handlers']['sql'] = {
    'class': 'plugins.sql_logger.SQLAlchemyLogHandler',
    'level': 'INFO',
    'formatter': 'airflow', 
}


LOGGING_CONFIG["loggers"]["airflow.task"]["handlers"].append("sql")