from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os

warehouse_path = os.path.abspath("data/warehouse")
print(warehouse_path)

spark = SparkSession.builder \
    .appName("tests") \
    .config("spark.hadoop.hadoop.native.lib", "false") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.0") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", warehouse_path) \
    .getOrCreate()

def test_table_and_column_comments():
    tables_df = spark.sql("SHOW TABLES IN local.bronze")\
    .withColumn("fullTableName", F.concat_ws(".", F.col("nameSpace"), F.col("tableName")))\
    .select("fullTableName")


    tables = [row["fullTableName"] for row in tables_df.collect()]
    
    for table in tables:
        full_table = f"local.{table}"
        
        # Check for table comment
        table_info = spark.sql(f"DESCRIBE TABLE EXTENDED {full_table}").collect()
        comment_line = [row for row in table_info if row.col_name == "Comment"]
        assert comment_line and comment_line[0].data_type.strip(), f"Missing table comment for {full_table}"

        # Check for column comments
        table_column_info = spark.sql(f"DESCRIBE TABLE {full_table}").collect()
        column_names = [row.col_name for row in table_column_info if row.col_name and not row.col_name.startswith("#")]
        for row in table_info:
            if row.data_type != "" and row.col_name in column_names:
                assert row.comment and row.comment.strip(), f"Missing column comment for {full_table}.{row.col_name}"

    print("✅ All tables and columns have comments.")

test_table_and_column_comments()