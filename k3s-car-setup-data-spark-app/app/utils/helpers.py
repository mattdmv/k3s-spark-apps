import datetime
from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from typing import List
from parsing_config import ColumnParsing, ParsingConfig

def parse_single_array_type_col(col: Column, column_parsing: ColumnParsing) -> List[Column]:
    return [
        col[i].alias(column_parsing.derivative_columns_names[i]) for i in range(len(column_parsing.suffixes))
    ]

def parse_array_type_cols(df: DataFrame, parsing_config: ParsingConfig) -> List[Column]:
    derivative_columns = []
    
    for column_parsing in parsing_config.parsing_rules:
        derivative_columns.extend(
            parse_single_array_type_col(
                col=df[column_parsing.name], 
                column_parsing=column_parsing
            )
        )
    
    return derivative_columns 

def to_string(col_name: str) -> Column:
    return F.col(col_name).cast(StringType())

def date_components_filter(date: datetime.date) -> Column:
    """Create filter by `year`, `month` and `day` columns.
    
    Args:
        date (datetime.date): date used as a filter
    Returns:
        Column: filter 
    """
    return (
        (F.col("year") == date.year)
        & (F.col("month") == date.month)
        & (F.col("day") == date.day)
    )

def filter_by_date(df: DataFrame, date: datetime.date) -> DataFrame:
    """Filter DataFrame by `year`, `month` and `day` columns.
    
    Returns: 
        DataFrame: spark DataFrame with filtered data
    """
    return df.filter(date_components_filter(date))