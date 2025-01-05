from session_data_spark_job import SessionDataSparkJob

job = SessionDataSparkJob(
    job_name="session-data-compaction-job",
    destination_table="nessie.compacted.session_ice"
)

job.run()