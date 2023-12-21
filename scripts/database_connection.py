from sqlalchemy import create_engine, pool
import logging

logging.basicConfig(level=logging.info)
logger = logging.getLogger(__name__)


class DatabaseConnection:
    def __init__(self):
        self.db_url = "postgresql://airflow:airflow@postgres/airflow"
        self.engin = create_engine(self.db_url)
    
    def connect(self):
        try:
            connection = self.engin.connect()
            logger.info('connected succesfully ')
            return connection
        except:
            logger.error('conndection to database failed')
