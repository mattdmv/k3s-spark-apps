import datetime
from pyspark.sql import DataFrame
from typing import List
from utils.helpers import filter_by_date
from utils.job import SparkJob
from constants import PARTITION_COLS, LAPS_DATA_S3_PATH
from laps_select_columns import LAPS_DATA_SELECT_COLS


class LapsDataSparkJob(SparkJob):
    def __init__(self, job_name: str, destination_table: str) -> None:
        super().__init__(job_name, destination_table)
        self.select_cols = LAPS_DATA_SELECT_COLS

    def load_parquet_files(self, parquet_file_s3_locations: str) -> DataFrame:
        return self.spark.read.parquet(parquet_file_s3_locations)
    
    def run_for_date(self, processing_date: datetime.date) -> None:
        df = self.load_parquet_files(LAPS_DATA_S3_PATH)

        # filter by date
        df = filter_by_date(df, processing_date)
        
        # keep only select cols
        df_final = df.select(*self.select_cols).orderBy("SessionTime")

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