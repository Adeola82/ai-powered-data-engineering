from pyspark import pipelines as dp


@dp.table(
    comment="Raw customer tiers data ingested from JSON volume via Auto Loader"
)
def bronze_customer_tiers():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("multiLine", "true")
        .load("/Volumes/demo/loan_io/customer_tiers/")
    )
