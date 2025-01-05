from telemetry_data_spark_job import TelemetryDataSparkJob

job = TelemetryDataSparkJob(
    job_name="telemetry-data-compaction-job",
    destination_table="nessie.compacted.telemetry_ice"
)

job.run()