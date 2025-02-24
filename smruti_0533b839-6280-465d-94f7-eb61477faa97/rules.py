from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan, length, lit
import logging

# Initialize Spark session
spark = SparkSession.builder.appName("CallCenterDataQuality").getOrCreate()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load data into DataFrames
def load_data(file_path):
    try:
        return spark.read.csv(file_path, header=True, inferSchema=True)
    except Exception as e:
        logger.error(f"Error loading data from {file_path}: {e}")
        return None

call_records_df = load_data("path_to_call_records.csv")
customer_interactions_df = load_data("path_to_customer_interactions.csv")
agents_df = load_data("path_to_agents.csv")
call_queue_df = load_data("path_to_call_queue.csv")
call_dispositions_df = load_data("path_to_call_dispositions.csv")
agent_performance_df = load_data("path_to_agent_performance.csv")
customer_feedback_df = load_data("path_to_customer_feedback.csv")

# Define quality check functions
def check_completeness(df, columns):
    logger.info(f"Checking completeness for columns: {columns}")
    return df.select([count(when(col(c).isNull() | isnan(c) | (length(col(c)) == 0), c)).alias(c) for c in columns])

def check_uniqueness(df, column):
    logger.info(f"Checking uniqueness for column: {column}")
    return df.groupBy(column).count().filter(col("count") > 1)

def check_valid_values(df, column, valid_values):
    logger.info(f"Checking valid values for column: {column}")
    return df.filter(~col(column).isin(valid_values))

def check_referential_integrity(df, column, ref_df, ref_column):
    logger.info(f"Checking referential integrity for column: {column}")
    return df.join(ref_df, df[column] == ref_df[ref_column], "left_anti")

# Apply quality checks
def apply_quality_checks():
    # Call_Records Table
    call_records_completeness = check_completeness(call_records_df, ["call_id", "timestamp", "duration", "caller_id"])
    call_records_uniqueness = check_uniqueness(call_records_df, "call_id")
    call_records_valid_call_type = check_valid_values(call_records_df, "call_type", ["inbound", "outbound"])

    # Customer_Interactions Table
    customer_interactions_completeness = check_completeness(customer_interactions_df, ["interaction_id", "customer_id", "interaction_type"])
    customer_interactions_valid_type = check_valid_values(customer_interactions_df, "interaction_type", ["call", "email", "chat"])
    customer_interactions_integrity = check_referential_integrity(customer_interactions_df, "customer_id", customer_feedback_df, "customer_id")

    # Agents Table
    agents_completeness = check_completeness(agents_df, ["agent_id", "name", "shift_details"])
    agents_uniqueness = check_uniqueness(agents_df, "agent_id")

    # Call_Queue Table
    call_queue_completeness = check_completeness(call_queue_df, ["queue_id", "call_id", "wait_time"])
    call_queue_integrity = check_referential_integrity(call_queue_df, "call_id", call_records_df, "call_id")

    # Call_Dispositions Table
    call_dispositions_completeness = check_completeness(call_dispositions_df, ["disposition_id", "call_id", "status"])
    call_dispositions_integrity = check_referential_integrity(call_dispositions_df, "call_id", call_records_df, "call_id")

    # Agent_Performance Table
    agent_performance_completeness = check_completeness(agent_performance_df, ["agent_id", "number_of_calls", "average_handling_time"])
    agent_performance_integrity = check_referential_integrity(agent_performance_df, "agent_id", agents_df, "agent_id")

    # Customer_Feedback Table
    customer_feedback_completeness = check_completeness(customer_feedback_df, ["feedback_id", "customer_id", "feedback_rating"])
    customer_feedback_integrity = check_referential_integrity(customer_feedback_df, "customer_id", customer_interactions_df, "customer_id")

    # Show results
    call_records_completeness.show()
    call_records_uniqueness.show()
    call_records_valid_call_type.show()

    customer_interactions_completeness.show()
    customer_interactions_valid_type.show()
    customer_interactions_integrity.show()

    agents_completeness.show()
    agents_uniqueness.show()

    call_queue_completeness.show()
    call_queue_integrity.show()

    call_dispositions_completeness.show()
    call_dispositions_integrity.show()

    agent_performance_completeness.show()
    agent_performance_integrity.show()

    customer_feedback_completeness.show()
    customer_feedback_integrity.show()

apply_quality_checks()

# Stop Spark session
spark.stop()