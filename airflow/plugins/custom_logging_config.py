import copy
from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG

LOGGING_CONFIG = copy.deepcopy(DEFAULT_LOGGING_CONFIG)

LOGGING_CONFIG['handlers']['sql'] = {
    'class': 'plugins.sql_logger.SQLAlchemyLogHandler',
    'level': 'INFO',
}

# Add to root if needed; extend specific loggers
for logger_name in ["airflow.task"]:
    if logger_name in LOGGING_CONFIG["loggers"]:
        LOGGING_CONFIG["loggers"][logger_name]["handlers"].append("sql")