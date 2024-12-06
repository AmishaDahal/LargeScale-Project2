import unittest
import pyspark
from pyspark.sql import SparkSession, functions as F
import os
from mutuallink import create_unique_mutual_links ,compute_bidirectional_connected_components
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, BooleanType
from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame

class DFIOSynth:
    def __init__(self, spark, workspace="test-workspace"):
        self.spark = spark
        self.workspace = os.path.abspath(workspace)

    def read(self, path):
        """Reads a JSON file from the test workspace."""
        full_path = os.path.normpath(os.path.join(self.workspace, path))
        print(f"Full path to the file: {full_path}")

        try:
            return self.spark.read.json(full_path)
        except Exception as e:
            print(f"Error reading from {full_path}: {str(e)}")
            return None

    def write(self, dataframe, path):
        """Writes a DataFrame to a JSON file in the test workspace."""
        full_path = os.path.normpath(os.path.join(self.workspace, path))
        print(f"Full path to the file: {full_path}")


        try:
            # Ensure the directory exists
            os.makedirs(os.path.dirname(full_path), exist_ok=True)

            # Write DataFrame to JSON
            dataframe.write.json(full_path, mode='overwrite')
            print(f"Data successfully written to {full_path}")
        except Exception as e:
            print(f"Error writing to {full_path}: {str(e)}")

    
def read_json(spark, path,schema=None):
    """Reads JSON data from a local directory."""
    full_path = os.path.abspath(f"./test-data/{path}.jsonl")  # Specify the file extension
    print(f"Reading from: {full_path}")  # Print the full path for debugging
    return spark.read.option("multiline", "true").json(full_path)



exp_links = frozenset((
    (1, 2), 
    (2,23),
    (5,9),
    (1,13),
    (15,17),
    (16,17),
    (17,25),
    (27,31),
    (23,32),
    (32, 33)
))



exp_components = frozenset((
    frozenset({1, 2, 13, 23, 32, 33}),
    frozenset({5, 9}),
    frozenset({15, 16, 17, 25}),
    frozenset({27, 31}),
))


class TestMutualLinks(unittest.TestCase):
    """Unit tests for verifying mutual links and components functionality."""
    
    def test_create_links_and_components(self):
        """Tests the create_links and components functionality."""
        
        # Initialize a local SparkSession for testing
        spark = (
            SparkSession.builder
            .master("local")  # Use all available cores
            .appName("Wikipedia Components")
            .getOrCreate()
        )
        # Initialize the helper class for I/O operations
        dfio = DFIOSynth(spark)

        # Define schema for each table
        linktarget_schema = StructType([
            StructField("lt_id", LongType(), True),
            StructField("lt_title", StringType(), True),
            StructField("lt_namespace", LongType(), True)
        ])

        page_schema = StructType([
            StructField("page_id", LongType(), True),
            StructField("page_title", StringType(), True),
            StructField("page_namespace", LongType(), True),
            StructField("page_content_model", StringType(), True),
            StructField("page_is_redirect", BooleanType(), True)
        ])

        redirect_schema = StructType([
            StructField("rd_from", LongType(), True),
            StructField("rd_namespace", LongType(), True),
            StructField("rd_title", StringType(), True),
            StructField("rd_fragment", StringType(), True)
        ])

        pagelinks_schema = StructType([
            StructField("pl_from", LongType(), True),
            StructField("pl_from_namespace", LongType(), True),
            StructField("pl_target_id", LongType(), True)
        ])
        

       # Read the JSON files using the read_json function with schema
        linktarget_df = read_json(spark, "linktarget", linktarget_schema)  # Using schema
        page_df = read_json(spark, "page", page_schema)  # Using schema
        redirect_df = read_json(spark, "redirect", redirect_schema)  # Using schema
        pagelinks_df = read_json(spark, "pagelinks", pagelinks_schema)  # Using schema

        # Print DataFrames to verify the content
        print("Linktarget DataFrame:")
        linktarget_df.show(truncate=False)
        
        print("Page DataFrame:")
        page_df.show(truncate=False)

        print("Redirect DataFrame:")
        redirect_df.show(truncate=False)

        print("Pagelinks DataFrame:")
        pagelinks_df.show(truncate=False)

        # Now, call the create_unique_mutual_links function with the DataFrames
        mutual_links = create_unique_mutual_links(
            spark,
            linktarget_df,
            page_df,
            redirect_df,
            pagelinks_df
        )
        print("showing my mutual links as|:")
        mutual_links.show()

        # Write the generated mutual links to the workspace for inspection
        dfio.write(mutual_links, "mutual_links")

       

        # #Assert that the actual mutual links match the expected ones
        actual_links = frozenset(tuple(r) for r in mutual_links.collect())
        self.assertEqual(actual_links, exp_links)




        # Generate connected components using the components function
        edges_df = mutual_links.selectExpr("PageA as src", "PageB as dst")
        cc = compute_bidirectional_connected_components(spark,
            edges_df,dfio)
        #cc = compute_bidirectional_connected_components(spark, edges)
        cc.show()
        

        # Write the connected components to the workspace for inspection
        dfio.write(cc, "wikipedia_components")



        # Group and collect the components as frozensets for comparison
        groups = frozenset(
            frozenset(c.members) for c in 
            cc.groupBy("component").agg(F.expr("collect_list(vertex) as members")).collect()
        )
        self.assertEqual(groups, exp_components)


# Run the tests
if __name__ == "__main__":
    unittest.main()
