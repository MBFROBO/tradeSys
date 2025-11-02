import logging
import re
from airflow import settings  # ← Ключевой импорт для Airflow 2.0+
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.session import provide_session
from airflow.utils.db import create_session
from airflow.models.base import Base
from sqlalchemy import Column, Integer, String, Text, DateTime
from datetime import datetime

class TaskLog(Base):
    __tablename__ = "task_logs"
    __table_args__ = {"extend_existing": True}

    id = Column(Integer, primary_key=True)
    dag_id = Column(String(250))
    task_id = Column(String(250))
    log_level = Column(String(50))
    message = Column(Text)
    timestamp = Column(DateTime, default=datetime.utcnow)

def log_task_message(dag_id, task_id, log_level, message):
    with create_session() as session:
        entry = TaskLog(
            dag_id=dag_id,
            task_id=task_id,
            log_level=log_level,
            message=message,
        )
        session.add(entry)
        session.commit()

class SQLAlchemyLogHandler(logging.Handler):
    def emit(self, record):
        message = self.format(record)
        
        ti = getattr(settings, 'task_instance', None)
        if ti:
            dag_id = ti.dag_id
            task_id = ti.task_id
        else:
            match = re.search(r"ti=<TaskInstance: (\w+)\.(\w+)", message)
            if match:
                dag_id, task_id = match.groups()
            else:
                dag_id = task_id = 'unknown'
        
        log_task_message(
            dag_id=dag_id,
            task_id=task_id,
            log_level=record.levelname,
            message=message,
        )

class SqlLoggerPlugin(AirflowPlugin):
    name = "sql_logger"
    models = [TaskLog]

    @provide_session
    def on_load(self, session=None):
        if not session.bind.dialect.has_table(session.bind.connect(), "task_logs"):
            TaskLog.__table__.create(bind=session.bind, checkfirst=True)