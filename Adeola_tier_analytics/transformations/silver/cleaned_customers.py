from pyspark import pipelines as dp
from pyspark.sql.functions import col, explode, to_timestamp


@dp.table(
    comment="Cleaned and flattened customer data with exploded array and parsed timestamps"
)
def silver_customers():
    return (
        spark.readStream.table("bronze_customer_tiers")
        .select(explode(col("customers")).alias("customer"))
        .select(
            col("customer.customer_id").alias("customer_id"),
            col("customer.first_name").alias("first_name"),
            col("customer.last_name").alias("last_name"),
            col("customer.email").alias("email"),
            col("customer.phone").alias("phone"),
            col("customer.loyalty_tier").alias("loyalty_tier"),
            col("customer.total_spent").alias("total_spent"),
            to_timestamp(col("customer.created_at")).alias("created_at"),
            col("customer.address.street").alias("street"),
            col("customer.address.city").alias("city"),
            col("customer.address.state").alias("state"),
            col("customer.address.postal_code").alias("postal_code"),
            col("customer.address.neighborhood").alias("neighborhood"),
            col("customer.address.country").alias("country"),
        )
    )
