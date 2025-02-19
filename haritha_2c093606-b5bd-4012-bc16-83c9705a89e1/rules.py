from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, is_null, lit, when

def check_completeness(df, required_fields):
    for field in required_fields:
        df = df.with_column(f"{field}_is_complete", ~is_null(col(field)))
    return df

def check_accuracy(df, field, valid_values):
    return df.with_column(f"{field}_is_accurate", col(field).isin(valid_values))

def check_format(df, field, format_regex):
    return df.with_column(f"{field}_is_formatted", col(field).rlike(format_regex))

def check_consistency(df, field, related_df, related_field):
    return df.join(related_df, df[field] == related_df[related_field], "left_anti").select(df["*"])

def check_uniqueness(df, field):
    return df.group_by(field).count().filter(col("count") > 1).select(field)

def check_timeliness(df, field, max_date):
    return df.with_column(f"{field}_is_timely", col(field) <= lit(max_date))

def apply_quality_rules(session, calls_df, agents_df, customers_df, interactions_df, transactions_df):
    # Calls Table
    calls_df = check_completeness(calls_df, ["call_id", "timestamp", "duration", "agent_id", "customer_id", "call_type", "call_outcome"])
    calls_df = check_accuracy(calls_df, "call_type", ["inbound", "outbound"])
    calls_df = check_format(calls_df, "timestamp", r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")
    calls_df = check_consistency(calls_df, "agent_id", agents_df, "agent_id")
    calls_df = check_consistency(calls_df, "customer_id", customers_df, "customer_id")
    calls_df = check_uniqueness(calls_df, "call_id")
    calls_df = check_timeliness(calls_df, "timestamp", "2023-12-31 23:59:59")

    # Agents Table
    agents_df = check_completeness(agents_df, ["agent_id", "name", "shift", "team"])
    agents_df = check_accuracy(agents_df, "shift", ["morning", "afternoon", "night"])
    agents_df = check_uniqueness(agents_df, "agent_id")

    # Customers Table
    customers_df = check_completeness(customers_df, ["customer_id", "name", "contact_info"])
    customers_df = check_format(customers_df, "contact_info", r"^[\w\.-]+@[\w\.-]+\.\w{2,4}$")
    customers_df = check_uniqueness(customers_df, "customer_id")

    # Interactions Table
    interactions_df = check_completeness(interactions_df, ["interaction_id", "type", "timestamp", "agent_id", "customer_id"])
    interactions_df = check_accuracy(interactions_df, "type", ["email", "chat", "call"])
    interactions_df = check_consistency(interactions_df, "agent_id", agents_df, "agent_id")
    interactions_df = check_consistency(interactions_df, "customer_id", customers_df, "customer_id")
    interactions_df = check_uniqueness(interactions_df, "interaction_id")

    # Transactions Table
    transactions_df = check_completeness(transactions_df, ["transaction_id", "customer_id", "amount", "date", "status"])
    transactions_df = check_accuracy(transactions_df, "status", ["pending", "completed", "failed"])
    transactions_df = check_consistency(transactions_df, "customer_id", customers_df, "customer_id")
    transactions_df = check_uniqueness(transactions_df, "transaction_id")

    return calls_df, agents_df, customers_df, interactions_df, transactions_df