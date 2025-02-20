from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, is_null, lit, when, count, row_number
from snowflake.snowpark.window import Window

def check_completeness(session, table_name, required_columns):
    df = session.table(table_name)
    for column in required_columns:
        df = df.with_column(f"quality_check_{column}_is_complete", ~is_null(col(column)))
    return df

def check_accuracy(session, table_name, column, valid_values):
    df = session.table(table_name)
    df = df.with_column(f"quality_check_{column}_is_accurate", col(column).isin(valid_values))
    return df

def check_uniqueness(session, table_name, column):
    df = session.table(table_name)
    window_spec = Window.partition_by(column)
    df = df.with_column(f"quality_check_{column}_is_unique", row_number().over(window_spec) == 1)
    return df

def check_consistency(session, table_name, column, reference_table, reference_column):
    df = session.table(table_name)
    ref_df = session.table(reference_table)
    df = df.join(ref_df, df[column] == ref_df[reference_column], "left")
    df = df.with_column(f"quality_check_{column}_is_consistent", ~is_null(ref_df[reference_column]))
    return df

def check_timeliness(session, table_name, column, max_date):
    df = session.table(table_name)
    df = df.with_column(f"quality_check_{column}_is_timely", col(column) <= lit(max_date))
    return df

def apply_quality_checks(session):
    # Call_Details Table
    call_details_required_columns = ["call_id", "timestamp", "duration", "agent_id", "customer_id", "call_type", "call_outcome"]
    call_details_df = check_completeness(session, "Call_Details", call_details_required_columns)
    call_details_df = check_accuracy(session, "Call_Details", "call_type", ["inbound", "outbound"])
    call_details_df = check_uniqueness(session, "Call_Details", "call_id")
    call_details_df = check_consistency(session, "Call_Details", "agent_id", "Agent_Details", "agent_id")
    call_details_df = check_consistency(session, "Call_Details", "customer_id", "Customer_Details", "customer_id")
    call_details_df = check_timeliness(session, "Call_Details", "timestamp", "2023-12-31")

    # Agent_Details Table
    agent_details_required_columns = ["agent_id", "name", "department", "shift_details"]
    agent_details_df = check_completeness(session, "Agent_Details", agent_details_required_columns)
    agent_details_df = check_accuracy(session, "Agent_Details", "department", ["sales", "support", "technical"])
    agent_details_df = check_uniqueness(session, "Agent_Details", "agent_id")

    # Customer_Details Table
    customer_details_required_columns = ["customer_id", "name", "contact_info"]
    customer_details_df = check_completeness(session, "Customer_Details", customer_details_required_columns)
    customer_details_df = check_uniqueness(session, "Customer_Details", "customer_id")

    # Call_Queue Table
    call_queue_required_columns = ["queue_id", "call_id", "position", "wait_time"]
    call_queue_df = check_completeness(session, "Call_Queue", call_queue_required_columns)
    call_queue_df = check_consistency(session, "Call_Queue", "call_id", "Call_Details", "call_id")

    # Call_Resolution Table
    call_resolution_required_columns = ["resolution_id", "call_id", "resolution_type", "resolution_time"]
    call_resolution_df = check_completeness(session, "Call_Resolution", call_resolution_required_columns)
    call_resolution_df = check_accuracy(session, "Call_Resolution", "resolution_type", ["resolved", "unresolved"])
    call_resolution_df = check_consistency(session, "Call_Resolution", "call_id", "Call_Details", "call_id")

    return {
        "Call_Details": call_details_df,
        "Agent_Details": agent_details_df,
        "Customer_Details": customer_details_df,
        "Call_Queue": call_queue_df,
        "Call_Resolution": call_resolution_df
    }