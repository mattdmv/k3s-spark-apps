import datetime
from pyspark.sql import DataFrame
from typing import List
from utils.helpers import filter_by_date, parse_array_type_cols
from utils.job import SparkJob
from parsing_config import ColumnParsing, ParsingConfig
from constants import PARTITION_COLS, MOTION_DATA_S3_PATH, WHEEL_ARRAY
from motion_select_columns import MOTION_DATA_SELECT_COLS, MOTION_DATA_PARSE_COLS


class MotionDataSparkJob(SparkJob):
    def __init__(self, job_name: str, destination_table: str) -> None:
        super().__init__(job_name, destination_table)
        self.select_cols = MOTION_DATA_SELECT_COLS
        self.parse_cols = MOTION_DATA_PARSE_COLS

    def load_parquet_files(self, parquet_file_s3_locations: str) -> DataFrame:
        return self.spark.read.parquet(parquet_file_s3_locations)
    
    def run_for_date(self, processing_date: datetime.date) -> None:
        df = self.load_parquet_files(MOTION_DATA_S3_PATH)

        # filter by date
        df = filter_by_date(df, processing_date)
        
        # get parsing config
        parsing_config = self.parsing_config

        # create derivative columns 
        derivative_columns = parse_array_type_cols(df, parsing_config)

        # keep select cols and derivative cols
        df_final = df.select(*self.select_cols, *derivative_columns).orderBy("SessionTime")

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

    @property
    def parsing_config(self) -> ParsingConfig:
        return ParsingConfig(
            [ColumnParsing(column, WHEEL_ARRAY) for column in self.parse_cols]
            )