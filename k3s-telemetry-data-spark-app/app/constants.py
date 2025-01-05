import datetime


WHEEL_ARRAY = ["RL", "RR", "FL", "FR"]

YEAR = "year"
MONTH = "month"
DAY = "day"

PARTITION_COLS = [YEAR, MONTH, DAY]

DEFAULT_START_DATE = datetime.date(2024, 9, 19)

TELEMETRY_DATA_S3_PATH = "s3a://test-bucket/topics/f1-telemetry-raw/packetId=6"
LAP_DATA_S3_PATH = "s3a://test-bucket/topics/f1-telemetry-raw/packetId=2"
CAR_SETUP_DATA_S3_PATH = "s3a://test-bucket/topics/f1-telemetry-raw/packetId=5"
MOTION_DATA_S3_PATH = "s3a://test-bucket/topics/f1-telemetry-raw/packetId=0"
SESSION_DATA_S3_PATHS = ["s3a://test-bucket/topics/f1-telemetry-raw/packetId=1", "s3a://test-bucket/topics/f1-telemetry-raw/packetId=4"]