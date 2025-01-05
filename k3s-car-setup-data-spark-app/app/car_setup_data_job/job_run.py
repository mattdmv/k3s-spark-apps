from car_setup_data_spark_job import CarSetupDataSparkJob

job = CarSetupDataSparkJob(
    job_name="car-setup-data-compaction-job",
    destination_table="nessie.compacted.car_setup_ice"
)

job.run()