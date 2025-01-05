import datetime
from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


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