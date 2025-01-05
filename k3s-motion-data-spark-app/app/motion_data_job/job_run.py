from motion_data_spark_job import MotionDataSparkJob

job = MotionDataSparkJob(
    job_name="motion-data-compaction-job",
    destination_table="nessie.compacted.car_motion_ice"
)

job.run()