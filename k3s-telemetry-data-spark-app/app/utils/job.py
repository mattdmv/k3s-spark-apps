import datetime
import logging
from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame, Column 
from pyspark.sql import functions as F
from pyspark.sql.types import DateType
from pyspark.sql.utils import AnalysisException
from helpers import to_string
from constants import YEAR, MONTH, DAY, DEFAULT_START_DATE


class BaseJob:
    def __init__(self, job_name: str, destination_table: str) -> None:
        self.job_name = job_name
        self.destination_table = destination_table


class SparkJob(BaseJob, ABC):
    def __init__(self, job_name: str, destination_table: str) -> None:
        super().__init__(job_name, destination_table)
        self.spark = self._create_spark_session()
        self.logger = logging.getLogger(job_name)

    def _create_spark_session(self) -> SparkSession:
        return SparkSession.builder.appName(self.job_name).getOrCreate()
    
    @property
    def min_processing_date(self) -> datetime.date:
        date_col = F.concat_ws(
            "-",
            to_string(YEAR),
            to_string(MONTH),
            to_string(DAY),    
        ).cast(DateType())

        try:
            last_date = self.get_max_date_from_table(self.destination_table, date_col)
            if last_date is None:
                return DEFAULT_START_DATE
                
            start_date = last_date + datetime.timedelta(days=1)
            return start_date
        
        except AnalysisException:
            return DEFAULT_START_DATE

    @property
    def max_processing_date(self) -> datetime.date:
        """Last date for which to process the data, which is current date minus one day.

        Returns:
            datetime.date: last date
        """
        return datetime.date.today() - datetime.timedelta(days=1)
    
    def get_max_date_from_table(self, table_name: str, date_column: Column) -> datetime.date:
        df = self.spark.table(table_name).select(F.max(date_column).alias("last_date"))
        last_date_row = df.first()
        return last_date_row["last_date"]
    
    def load_parquet_files(self, parquet_file_s3_location: str) -> DataFrame:
        return self.spark.read.parquet(parquet_file_s3_location)
    
    @abstractmethod
    def run_for_date(self, processing_date: datetime.date) -> None:
        raise NotImplementedError
    
    def run(self) -> None:
        """Run job for all days between `min_processing_date` and `max_processing_date`"""
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

        self.logger.info('Started')

        start_date = self.min_processing_date
        end_date = self.max_processing_date

        days = (end_date - start_date).days + 1

        for i in range(days):
            processing_date = start_date + datetime.timedelta(days=i)
            self.run_for_date(processing_date)
            self.logger.info(f'Processed data for: {processing_date}')

        self.logger.info('Finished')