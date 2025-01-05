import datetime
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import List
from utils.helpers import filter_by_date
from utils.job import SparkJob
from constants import PARTITION_COLS, SESSION_DATA_S3_PATHS
from select_columns import SESSION_DATA_SELECT_COLS, PARTICIPANT_DATA_SELECT_COLS

class SessionDataSparkJob(SparkJob):
    def __init__(self, job_name: str, destination_table: str) -> None:
        super().__init__(job_name, destination_table)
        self.select_cols_session = SESSION_DATA_SELECT_COLS
        self.select_cols_participant = PARTICIPANT_DATA_SELECT_COLS

    def load_parquet_files(self, parquet_file_s3_locations: List[str]) -> DataFrame:
        return [self.spark.read.parquet(s3_path) for s3_path in parquet_file_s3_locations]
    
    def run_for_date(self, processing_date: datetime.date) -> None:
        dfs = self.load_parquet_files(SESSION_DATA_S3_PATHS)

        # assign DataFrames from list to separate variables and filter by date
        df_session = filter_by_date(dfs[0], processing_date)
        df_participant = filter_by_date(dfs[1], processing_date)

        # aggregate df_session by `sessionUID` column
        agg_cols_session = self.select_cols_session[1:]
        aggs_session = [F.max(col).alias(col) for col in agg_cols_session]

        df_session_agg = df_session.select(*self.select_cols_session).groupBy("sessionUID").agg(*aggs_session)

        # aggregate df_participant by `sessionUID` column
        agg_cols_participant = self.select_cols_participant[1:]
        aggs_participant = [F.first(col).alias(col) for col in agg_cols_participant]
        
        df_participant_agg = df_participant.select(*self.select_cols_participant).groupBy("sessionUID").agg(*aggs_participant)

        # join aggregated dataframes on `sessionUID` column
        df_final = df_session_agg.join(df_participant_agg, "sessionUID")

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