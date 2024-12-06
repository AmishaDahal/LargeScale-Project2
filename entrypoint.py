from pyspark.sql import SparkSession,functions as F 
import os

print("Starting Spark Session")

spark = (
    SparkSession.builder
    #.remote("sc://localhost:15002")
    .appName("Wikipedia Mutual Links and connected Componets")
    .getOrCreate()
)

from mutuallink import DFIO,compute_bidirectional_connected_components,create_unique_mutual_links,parquet

print("calculating mutual links")

dfio =DFIO(spark)
# Load data using parquet
parquet(spark, "linktarget")
parquet(spark, "page")
parquet(spark, "redirect")
parquet(spark, "pagelinks")

# Fetch DataFrames from temporary views
linktarget_df = spark.table("linktarget")
page_df = spark.table("page")
redirect_df = spark.table("redirect")
pagelinks_df = spark.table("pagelinks")

# Call the function
mutual_links = create_unique_mutual_links(spark,
    linktarget_df,
    page_df,
    redirect_df,
    pagelinks_df
)


dfio.write(mutual_links, "mutual_links")
mutual_links=dfio.read("mutual_links")

print("starting connected components")
edges_df = mutual_links.selectExpr("PageA as src", "PageB as dst")
final_result = compute_bidirectional_connected_components(spark, edges_df,dfio)
dfio.write(final_result, "wikipedia_components")

print("componet stats")
component_sizes = (final_result.groupBy("component").agg(F.count("*").alias("size"))
        .orderBy(F.desc("size")))


print(f"Total components: {component_sizes.count()}")

print("Top 20 largest components:")
component_sizes.show()

print(f"Final_vertices and component:")
final_result.show()
print(f"Total vertices processed: {final_result.count()}")