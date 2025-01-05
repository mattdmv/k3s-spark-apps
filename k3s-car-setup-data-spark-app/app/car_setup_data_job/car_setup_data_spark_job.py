import datetime
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import List
from utils.helpers import filter_by_date
from utils.job import SparkJob
from constants import PARTITION_COLS, CAR_SETUP_DATA_S3_PATH
from select_columns import CAR_SETUP_DATA_SELECT_COLS


class CarSetupDataSparkJob(SparkJob):
    def __init__(self, job_name: str, destination_table: str) -> None:
        super().__init__(job_name, destination_table)
        self.select_cols = CAR_SETUP_DATA_SELECT_COLS

    def load_parquet_files(self, parquet_file_s3_locations: List[str]) -> DataFrame:
        return self.spark.read.parquet(parquet_file_s3_locations)
    
    def run_for_date(self, processing_date: datetime.date) -> None:
        df = self.load_parquet_files(CAR_SETUP_DATA_S3_PATH)

        # assign DataFrames from list to separate variables and filter by date
        df = filter_by_date(df, processing_date)

        # keep only select cols
        df = df.select(*self.select_cols).orderBy("sessionTime")

        df_final = df.dropDuplicates(col for col in self.select_cols if col not in ["sessionTime", "frameIdentifier"])

        self.write_partitions_to_table(
            df=df_final, 
            write_table=self.destination_table,
            partition_cols=PARTITION_COLS
            )

    def write_partitions_to_table(
            self, 
            df: DataFrame,
            write_table: str,
            partition_cols: List[str]
            ) -> None:
         df.write.format("iceberg") \
            .mode("append") \
            .option("write-format", "parquet") \
            .partitionBy(*partition_cols) \
            .saveAsTable(write_table)