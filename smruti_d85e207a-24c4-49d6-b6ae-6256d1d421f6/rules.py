from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, regexp_extract, when, lit, broadcast

# Initialize Spark session
spark = SparkSession.builder.appName("CallCenterDataQuality").getOrCreate()

# Load data
call_details_df = spark.read.csv("path_to_call_details.csv", header=True, inferSchema=True)
agent_details_df = spark.read.csv("path_to_agent_details.csv", header=True, inferSchema=True)
customer_details_df = spark.read.csv("path_to_customer_details.csv", header=True, inferSchema=True)
call_queue_df = spark.read.csv("path_to_call_queue.csv", header=True, inferSchema=True)
call_resolution_df = spark.read.csv("path_to_call_resolution.csv", header=True, inferSchema=True)

def check_completeness(df, required_columns):
    return df.withColumn("is_complete", 
                         when(reduce(lambda x, y: x & y, [col(c).isNotNull() for c in required_columns]), lit(True)).otherwise(lit(False)))

def check_uniqueness(df, column):
    return df.withColumn("is_unique", 
                         when(df.groupBy(column).count().filter("count > 1").count() == 0, lit(True)).otherwise(lit(False)))

def check_valid_values(df, column, valid_values):
    return df.withColumn(f"is_valid_{column}", 
                         when(col(column).isin(valid_values), lit(True)).otherwise(lit(False)))

def check_timestamp_format(df, column):
    return df.withColumn(f"is_valid_{column}", 
                         when(regexp_extract(col(column), r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$", 0) != "", lit(True)).otherwise(lit(False)))

# Quality Rules for Call_Details Table
call_details_df = check_completeness(call_details_df, ["call_id", "timestamp", "duration", "agent_id", "customer_id", "call_type", "call_outcome"])
call_details_df = check_valid_values(call_details_df, "call_type", ["inbound", "outbound"])
call_details_df = check_timestamp_format(call_details_df, "timestamp")
call_details_df = check_uniqueness(call_details_df, "call_id")

# Quality Rules for Agent_Details Table
agent_details_df = check_completeness(agent_details_df, ["agent_id", "name", "department"])
agent_details_df = check_uniqueness(agent_details_df, "agent_id")

# Quality Rules for Customer_Details Table
customer_details_df = check_completeness(customer_details_df, ["customer_id", "name", "contact_info"])
customer_details_df = check_uniqueness(customer_details_df, "customer_id")

# Quality Rules for Call_Queue Table
call_queue_df = check_completeness(call_queue_df, ["queue_id", "call_id", "position", "wait_time"])

# Quality Rules for Call_Resolution Table
call_resolution_df = check_completeness(call_resolution_df, ["resolution_id", "call_id", "resolution_type", "resolution_time"])

# Stop Spark session
spark.stop()