from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, regexp_extract, count, expr
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("CallCenterDataQuality").getOrCreate()

def check_completeness(df, columns):
    for column in columns:
        df = df.withColumn(f"{column}_completeness", when(col(column).isNull(), lit("Incomplete")).otherwise(lit("Complete")))
    return df

def check_uniqueness(df, column):
    window_spec = Window.partitionBy(column)
    df = df.withColumn(f"{column}_uniqueness", when(count(column).over(window_spec) > 1, lit("Duplicate")).otherwise(lit("Unique")))
    return df

def check_accuracy(df, column, pattern):
    df = df.withColumn(f"{column}_accuracy", when(regexp_extract(col(column), pattern, 0) == "", lit("Invalid")).otherwise(lit("Valid")))
    return df

def check_consistency(df, column, reference_df, reference_column):
    df = df.join(reference_df, df[column] == reference_df[reference_column], "left")
    df = df.withColumn(f"{column}_consistency", when(reference_df[reference_column].isNull(), lit("Inconsistent")).otherwise(lit("Consistent")))
    return df

def check_timeliness(df, column, min_value, max_value):
    df = df.withColumn(f"{column}_timeliness", when((col(column) < min_value) | (col(column) > max_value), lit("Outdated")).otherwise(lit("Timely")))
    return df

# Example usage for Calls table
calls_df = spark.read.csv("path_to_calls_data.csv", header=True, inferSchema=True)
calls_df = check_completeness(calls_df, ["call_id", "timestamp", "duration", "agent_id", "customer_id", "call_type", "call_outcome"])
calls_df = check_uniqueness(calls_df, "call_id")
calls_df = check_accuracy(calls_df, "call_type", "^(inbound|outbound)$")
calls_df = check_accuracy(calls_df, "call_outcome", "^(completed|missed|voicemail)$")

# Example usage for Agents table
agents_df = spark.read.csv("path_to_agents_data.csv", header=True, inferSchema=True)
agents_df = check_completeness(agents_df, ["agent_id", "name", "shift", "team"])
agents_df = check_uniqueness(agents_df, "agent_id")
agents_df = check_accuracy(agents_df, "shift", "^(morning|afternoon|night)$")

# Example usage for Customers table
customers_df = spark.read.csv("path_to_customers_data.csv", header=True, inferSchema=True)
customers_df = check_completeness(customers_df, ["customer_id", "name", "contact_details"])
customers_df = check_uniqueness(customers_df, "customer_id")
customers_df = check_accuracy(customers_df, "contact_details", "^[0-9]{10}$")  # Example pattern for phone numbers

# Example usage for Call_Logs table
call_logs_df = spark.read.csv("path_to_call_logs_data.csv", header=True, inferSchema=True)
call_logs_df = check_completeness(call_logs_df, ["call_id", "event_timestamps", "agent_notes"])
call_logs_df = check_consistency(call_logs_df, "call_id", calls_df, "call_id")

# Example usage for Feedback table
feedback_df = spark.read.csv("path_to_feedback_data.csv", header=True, inferSchema=True)
feedback_df = check_completeness(feedback_df, ["feedback_id", "call_id", "feedback_score"])
feedback_df = check_accuracy(feedback_df, "feedback_score", "^[1-5]$")

# Example usage for Queue_Stats table
queue_stats_df = spark.read.csv("path_to_queue_stats_data.csv", header=True, inferSchema=True)
queue_stats_df = check_completeness(queue_stats_df, ["queue_id", "average_wait_time", "calls_in_queue"])
queue_stats_df = queue_stats_df.withColumn("average_wait_time_accuracy", when(col("average_wait_time") < 0, lit("Invalid")).otherwise(lit("Valid")))

# Stop Spark session
spark.stop()