from sqlalchemy import create_engine, pool
import logging

logging.basicConfig(level=logging.info)
logger = logging.getLogger(__name__)


class DatabaseConnection:
    def connect(self):
        try:
            db_url = "postgresql://airflow:airflow@postgres/airflow"
            engin = create_engine(db_url)
            connection = engin.connect()
            logger.info('connected succesfully ')
            return connection
        except:
            logger.error('conndection to database failed')
            
