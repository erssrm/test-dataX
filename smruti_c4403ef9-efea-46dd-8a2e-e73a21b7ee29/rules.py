from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, regexp_extract, when, countDistinct

# Initialize Spark session
spark = SparkSession.builder.appName("CallCenterDataQuality").getOrCreate()

# Load data into DataFrames
call_records_df = spark.read.csv("path_to_call_records.csv", header=True, inferSchema=True)
agent_df = spark.read.csv("path_to_agent.csv", header=True, inferSchema=True)
customer_df = spark.read.csv("path_to_customer.csv", header=True, inferSchema=True)
queue_df = spark.read.csv("path_to_queue.csv", header=True, inferSchema=True)
feedback_df = spark.read.csv("path_to_feedback.csv", header=True, inferSchema=True)

# Function to check completeness
def check_completeness(df, columns):
    return df.withColumn("is_complete", reduce(lambda a, b: a & b, [col(c).isNotNull() for c in columns]))

# Quality rules for Call Records Table
call_records_df = check_completeness(call_records_df, ["call_id", "timestamp", "duration", "agent_id", "customer_id", "call_type", "call_outcome"])
call_records_df = call_records_df.withColumn("is_valid_call_type", col("call_type").isin("inbound", "outbound"))
call_records_df = call_records_df.withColumn("is_unique_call_id", countDistinct("call_id") == call_records_df.count())

# Quality rules for Agent Table
agent_df = check_completeness(agent_df, ["agent_id", "name", "shift_details"])
agent_df = agent_df.withColumn("is_unique_agent_id", countDistinct("agent_id") == agent_df.count())

# Quality rules for Customer Table
customer_df = check_completeness(customer_df, ["customer_id", "name", "contact_details"])
customer_df = customer_df.withColumn("is_unique_customer_id", countDistinct("customer_id") == customer_df.count())
customer_df = customer_df.withColumn("is_valid_contact", regexp_extract(col("contact_details"), r"^\+?[1-9]\d{1,14}$", 0) != "")

# Quality rules for Queue Table
queue_df = check_completeness(queue_df, ["queue_id", "queue_name", "metrics"])
queue_df = queue_df.withColumn("is_unique_queue_id", countDistinct("queue_id") == queue_df.count())

# Quality rules for Feedback Table
feedback_df = check_completeness(feedback_df, ["feedback_id", "customer_id", "call_id", "feedback_score"])
feedback_df = feedback_df.withColumn("is_unique_feedback_id", countDistinct("feedback_id") == feedback_df.count())
feedback_df = feedback_df.withColumn("is_valid_feedback_score", (col("feedback_score") >= 1) & (col("feedback_score") <= 5))

# Stop Spark session
spark.stop()