from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark import StorageLevel
import time
import os
import datetime
from pyspark.sql import DataFrame

class DFIO:
    def __init__(self, spark, workspace=None, local_write=False):
        self.spark = spark
        self.workspace = workspace or './test-workspace'  # Default to local path
        self.local_write = local_write 
         # Determine whether to write locally or in Spark
    
    def read(self,path):
       path=f"{os.environ['CS535_S3_WORKSPACE']}{path}"
       return self.spark.read.parquet(path)
    def write(self,df,path):
        workspace =os.environ['CS535_S3_WORKSPACE']
        df.write.mode("OVERWRITE").parquet(f"{workspace}{path}")

def parquet(spark,prefix):
    t = datetime.datetime.now()
    files = spark.read.parquet(f"s3://bsu-c535-fall2024-commons/arjun-workspace/{prefix}/")
    files.persist(StorageLevel.MEMORY_AND_DISK).createOrReplaceTempView(prefix)
    rows = spark.table(prefix).count()
    print(f"{prefix} - {rows} rows - elapsed {datetime.datetime.now() - t}")
    spark.table(prefix).printSchema()


def create_unique_mutual_links(spark,linktarget_df, page_df, redirect_df, pagelinks_df):
    if any(df is None for df in [linktarget_df, page_df, redirect_df, pagelinks_df]):
        raise ValueError("One or more input DataFrames are None.")
    

    # Step 2: Create lt_page view by joining redirect and page
    lt_page = redirect_df.join(
        page_df,
        (F.col("page_title") == F.col("rd_title")) & 
        (F.col("page_namespace") == F.col("rd_namespace"))
    ).select(
        F.col("rd_from").alias("redirect_source_id"),  
        F.col("page_id").alias("redirect_target_id")
    ).distinct()

    # Step 3: Join page and linktarget to create page_linktarget view
    page_linktarget = linktarget_df.join(
        page_df,
        (F.col("lt_title") == F.col("page_title")) & 
        (F.col("lt_namespace") == F.col("page_namespace")),
        how='inner'
    ).select(
        F.col("page_id").alias("page_id"),
        F.col("lt_id").alias("link_target_id"),
        F.col("page_namespace").alias("page_namespace")
    )
     # Step 4: Join page_linktarget with pagelinks to create page_linktarget_with_pagelink
    page_linktarget_with_pagelink = pagelinks_df.join(
        page_linktarget,
        F.col("pl_target_id") == F.col("link_target_id"),
        how='inner'
    ).select(
        F.col("pl_from").alias("pl_from_source_id"),
        F.col("page_id").alias("page_id_target_id")
    ).filter(F.col("page_namespace") == '0')

    # Step 5: Resolve source and target IDs to create resolved_links
    resolved_links = page_linktarget_with_pagelink.alias("pl").join(
        lt_page.alias("r1"),
        F.col("pl.pl_from_source_id") == F.col("r1.redirect_source_id"),
        how='left'
    ).join(
        lt_page.alias("r2"),
        F.col("pl.page_id_target_id") == F.col("r2.redirect_source_id"),
        how='left'
    ).select(
        F.coalesce(F.col("r1.redirect_target_id"), F.col("pl.pl_from_source_id")).alias("resolved_source_id"),
        F.coalesce(F.col("r2.redirect_target_id"), F.col("pl.page_id_target_id")).alias("resolved_target_id")
    ).filter(
        F.col("resolved_source_id") != F.col("resolved_target_id")
    ).distinct()

 # Step 6: Find mutual links
    resolved_links_1= resolved_links.alias("df1").join(
        resolved_links.alias("df2"),
        (F.col("df1.resolved_source_id") == F.col("df2.resolved_target_id")) & 
        (F.col("df1.resolved_target_id") == F.col("df2.resolved_source_id")),
        how="inner"
    ).select(
        F.col("df1.resolved_source_id").alias("PageA"),
        F.col("df1.resolved_target_id").alias("PageB")
    ).distinct()

    # Step 7: Filter for unique pairs
    mutual_links = resolved_links_1.filter(
        F.col("PageA") < F.col("PageB")
    ).distinct()
    return mutual_links

#project 2 conncected componet

def compute_bidirectional_connected_components(spark,
    edges_df,dfio, checkpoint_interval=3, max_iterations=1000, checkpoint_dir="checkpoint",
):
    print("Creating bidirectional edges")
    link_edges = edges_df.union(
        edges_df.select(F.col("dst").alias("src"), F.col("src").alias("dst"))
    ).distinct()
    link_edges.persist(StorageLevel.MEMORY_AND_DISK)

    print("Creating vertex list and initializing component IDs")
    active_nodes = (
        link_edges.select(F.col("src").alias("vertex"))
        .union(link_edges.select(F.col("dst").alias("vertex")))
        .distinct()
        .withColumn("component", F.col("vertex"))
    )
    active_nodes.persist(StorageLevel.MEMORY_AND_DISK)

    print("Starting computation of connected components")
    iteration = 0

    while True:
        start_time = time.time()

        # Step 3: Propagate component IDs to neighbors
        connected_nodes = link_edges.alias("edges").join(
            active_nodes.alias("nodes"),
            F.col("edges.src") == F.col("nodes.vertex"),
            "inner"
        ).select(
            F.col("edges.dst").alias("target_node"),
            F.col("nodes.component").alias("propagated_component")
        )

        # Step 4: Combine current and propagated components
        original_components = active_nodes.select(
            F.col("vertex").alias("node_id"),
            F.col("component").alias("current_component_id")
        )

        propagated_components = connected_nodes.select(
            F.col("target_node").alias("node_id"),
            F.col("propagated_component").alias("current_component_id")
        )

        merged_components = original_components.union(propagated_components)

        refined_nodes_to_smallest = merged_components.groupBy("node_id").agg(
            F.min("current_component_id").alias("smallest_component_id")
        ).persist(StorageLevel.MEMORY_AND_DISK)

        # Step 5: Detect changes
        updates_detected = refined_nodes_to_smallest.alias("refined").join(
            active_nodes.alias("active"),
            F.col("refined.node_id") == F.col("active.vertex")
        ).where(
            F.col("refined.smallest_component_id") != F.col("active.component")
        ).count()

        elapsed_time = time.time() - start_time
        print(f"Iteration {iteration + 1}: Changed nodes = {updates_detected}, Elapsed time = {elapsed_time:.6f}s")

        # Step 6: Update vertices for the next iteration
        active_nodes = refined_nodes_to_smallest.select(
            F.col("node_id").alias("vertex"),
            F.col("smallest_component_id").alias("component")
        )

        #dfio = DFIO(spark)

        # Step 7: Save a checkpoint
        if (iteration + 1) % checkpoint_interval == 0:
            checkpoint_path = f"{checkpoint_dir}/iteration_{iteration + 1}"
            # active_nodes.write.mode("overwrite").parquet(checkpoint_path)
            dfio.write(active_nodes,checkpoint_path)
            print(f"Checkpoint saved to: {checkpoint_path}")

            # Read back using DFIO
            active_nodes = dfio.read(checkpoint_path)  
            print(f"Checkpoint reloaded from: {checkpoint_path}")

        # Step 8: Stopping criteria
        if updates_detected == 0:
            print("Convergence reached.")
            break
        if iteration + 1 >= max_iterations:
            print("Max iterations reached.")
            break

        iteration += 1

    # Output: vertex and component columns
    final_result = active_nodes.select("vertex", "component")
    return final_result