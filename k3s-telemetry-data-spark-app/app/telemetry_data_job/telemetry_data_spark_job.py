import datetime
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import List
from utils.helpers import filter_by_date
from utils.job import SparkJob
from constants import PARTITION_COLS, TELEMETRY_DATA_S3_PATH
from telemetry_select_columns import TELEMETRY_DATA_SELECT_COLS


class TelemetryDataSparkJob(SparkJob):
    def __init__(self, job_name: str, destination_table: str) -> None:
        super().__init__(job_name, destination_table)
        self.select_cols_telemetry = TELEMETRY_DATA_SELECT_COLS

    def load_parquet_files(self, parquet_file_s3_location: str) -> DataFrame:
        return self.spark.read.parquet(parquet_file_s3_location)
    
    def run_for_date(self, processing_date: datetime.date) -> None:
        df = self.load_parquet_files(TELEMETRY_DATA_S3_PATH)

        # assign DataFrames from list to separate variables and filter by date
        df = filter_by_date(df, processing_date)

        # join aggregated dataframes on `sessionUID` column
        df_final = df.select(*self.select_cols_telemetry).orderBy("SessionTime")

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