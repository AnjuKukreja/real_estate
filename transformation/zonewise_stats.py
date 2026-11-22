import os
from pyspark.sql import SparkSession

def main():

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    rentals_path = os.path.join(project_root, "data", "rentals.csv")

    spark = SparkSession.builder.appName("Zonewise Stats").master("local[*]").getOrCreate()

    rentals_df = spark.read.option("header", "true").option("inferSchema", "true").csv(rentals_path)
    off_plan_df = spark.read.option("header", "true").option("inferSchema", "true").csv(os.path.join(project_root, "data", "off_plan.csv"))
    resale_df = spark.read.option("header", "true").option("inferSchema", "true").csv(os.path.join(project_root, "data", "secondary_sales.csv"))

    zonewise_stats_rentals = rentals_df.groupBy("zone").avg("annual_rent_usd").withColumnRenamed("avg(annual_rent_usd)", "avg_annual_rent_usd")
    zonewise_off_plan_df = off_plan_df.groupBy("zone").avg("price_usd").withColumnRenamed("avg(price_usd)", "avg_purchase_price_usd")
    zonewise_resale_df = resale_df.groupBy("zone").avg("price_usd").withColumnRenamed("avg(price_usd)", "avg_resale_price_usd")

    zonewise_stats_df = zonewise_stats_rentals.join(zonewise_off_plan_df, "zone", "outer").join(zonewise_resale_df, "zone", "outer")
    zonewise_stats_df.printSchema()
    zonewise_stats_df.show(10, truncate=False)

    zonewise_stats_df.write.mode("overwrite").option("header", "true").csv(os.path.join(project_root, "output", "zonewise_stats.csv"))

if __name__ == "__main__":
    main()

# the idea is to generate 3 results 1 for rental, 1 for off plan, 1 for resale and each result will compare the average price, (average size, and average price per square foot) for each zone. The results will be saved as separate CSV files in the output directory.