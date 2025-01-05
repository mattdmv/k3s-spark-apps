from laps_data_spark_job import LapsDataSparkJob

job = LapsDataSparkJob(
    job_name="laps-data-compaction-job",
    destination_table="nessie.compacted.laps_ice"
)

job.run()