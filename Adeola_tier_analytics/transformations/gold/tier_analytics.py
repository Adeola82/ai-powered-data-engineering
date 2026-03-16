from pyspark import pipelines as dp
from pyspark.sql.functions import col, count, sum, avg, min, max, round


@dp.materialized_view(
    comment="Customer tier analytics with aggregated metrics per loyalty tier"
)
def gold_tier_summary():
    return (
        spark.read.table("silver_customers")
        .groupBy("loyalty_tier")
        .agg(
            count("customer_id").alias("customer_count"),
            round(sum("total_spent"), 2).alias("total_revenue"),
            round(avg("total_spent"), 2).alias("avg_spend"),
            round(min("total_spent"), 2).alias("min_spend"),
            round(max("total_spent"), 2).alias("max_spend"),
        )
    )
