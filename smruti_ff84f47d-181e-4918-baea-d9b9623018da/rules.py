from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan, broadcast

# Initialize Spark session
spark = SparkSession.builder.appName("CallCenterDataQuality").getOrCreate()

def load_data(file_path):
    """Utility function to load data into a DataFrame."""
    return spark.read.csv(file_path, header=True, inferSchema=True)

# Load data into DataFrames
calls_df = load_data("path_to_calls_data.csv")
agents_df = load_data("path_to_agents_data.csv")
customers_df = load_data("path_to_customers_data.csv")
interactions_df = load_data("path_to_interactions_data.csv")
transactions_df = load_data("path_to_transactions_data.csv")

def check_completeness(df, columns):
    """Check completeness of specified columns in a DataFrame."""
    return df.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in columns])

def check_uniqueness(df, column):
    """Check uniqueness of a specified column in a DataFrame."""
    return df.groupBy(column).count().filter("count > 1")

def check_valid_values(df, column, valid_values):
    """Check if column values are within valid values."""
    return df.filter(~col(column).isin(valid_values))

def check_referential_integrity(df, column, ref_df, ref_column):
    """Check referential integrity between two DataFrames."""
    return df.join(broadcast(ref_df), df[column] == ref_df[ref_column], "left_anti")

# Check completeness
calls_completeness = check_completeness(calls_df, ["call_id", "timestamp", "duration", "agent_id", "customer_id", "call_type", "call_outcome"])
agents_completeness = check_completeness(agents_df, ["agent_id", "name", "shift", "team"])
customers_completeness = check_completeness(customers_df, ["customer_id", "name", "contact_details"])
interactions_completeness = check_completeness(interactions_df, ["interaction_id", "type", "timestamp", "agent_id", "customer_id"])
transactions_completeness = check_completeness(transactions_df, ["transaction_id", "customer_id", "amount", "date", "status"])

# Check uniqueness
calls_uniqueness = check_uniqueness(calls_df, "call_id")
agents_uniqueness = check_uniqueness(agents_df, "agent_id")
customers_uniqueness = check_uniqueness(customers_df, "customer_id")
interactions_uniqueness = check_uniqueness(interactions_df, "interaction_id")
transactions_uniqueness = check_uniqueness(transactions_df, "transaction_id")

# Check valid values
calls_valid_values = check_valid_values(calls_df, "call_type", ["inbound", "outbound"])
agents_valid_values = check_valid_values(agents_df, "shift", ["morning", "afternoon", "night"])
transactions_valid_values = check_valid_values(transactions_df, "status", ["completed", "pending", "failed"])

# Check referential integrity
calls_agent_ref_integrity = check_referential_integrity(calls_df, "agent_id", agents_df, "agent_id")
calls_customer_ref_integrity = check_referential_integrity(calls_df, "customer_id", customers_df, "customer_id")
interactions_agent_ref_integrity = check_referential_integrity(interactions_df, "agent_id", agents_df, "agent_id")
interactions_customer_ref_integrity = check_referential_integrity(interactions_df, "customer_id", customers_df, "customer_id")
transactions_customer_ref_integrity = check_referential_integrity(transactions_df, "customer_id", customers_df, "customer_id")

# Show results
calls_completeness.show()
agents_completeness.show()
customers_completeness.show()
interactions_completeness.show()
transactions_completeness.show()

calls_uniqueness.show()
agents_uniqueness.show()
customers_uniqueness.show()
interactions_uniqueness.show()
transactions_uniqueness.show()

calls_valid_values.show()
agents_valid_values.show()
transactions_valid_values.show()

calls_agent_ref_integrity.show()
calls_customer_ref_integrity.show()
interactions_agent_ref_integrity.show()
interactions_customer_ref_integrity.show()
transactions_customer_ref_integrity.show()

# Stop Spark session
spark.stop()