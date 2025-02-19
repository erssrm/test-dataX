from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, is_null, lit, when

def check_completeness(session: Session, table_name: str, required_columns: list):
    """
    Check for completeness of required columns in a table.
    
    :param session: Snowflake session object
    :param table_name: Name of the table to check
    :param required_columns: List of required columns to check for completeness
    :return: DataFrame with completeness check results
    """
    df = session.table(table_name)
    for column in required_columns:
        df = df.with_column(f"{column}_is_null", is_null(col(column)))
    return df

def check_accuracy(session: Session, table_name: str, column: str, valid_values: list):
    """
    Check for accuracy of a column against valid values.
    
    :param session: Snowflake session object
    :param table_name: Name of the table to check
    :param column: Column to check for accuracy
    :param valid_values: List of valid values for the column
    :return: DataFrame with accuracy check results
    """
    df = session.table(table_name)
    df = df.with_column(f"{column}_is_valid", col(column).isin(valid_values))
    return df

def check_consistency(session: Session, table_name: str, column: str, format_regex: str):
    """
    Check for consistency of a column against a format regex.
    
    :param session: Snowflake session object
    :param table_name: Name of the table to check
    :param column: Column to check for consistency
    :param format_regex: Regex pattern to check the column against
    :return: DataFrame with consistency check results
    """
    df = session.table(table_name)
    df = df.with_column(f"{column}_is_consistent", col(column).rlike(format_regex))
    return df

def check_uniqueness(session: Session, table_name: str, column: str):
    """
    Check for uniqueness of a column.
    
    :param session: Snowflake session object
    :param table_name: Name of the table to check
    :param column: Column to check for uniqueness
    :return: DataFrame with uniqueness check results
    """
    df = session.table(table_name)
    df = df.group_by(column).count().filter(col("count") > 1)
    return df

def check_referential_integrity(session: Session, table_name: str, column: str, ref_table: str, ref_column: str):
    """
    Check for referential integrity between two tables.
    
    :param session: Snowflake session object
    :param table_name: Name of the table to check
    :param column: Column in the table to check
    :param ref_table: Reference table for integrity check
    :param ref_column: Reference column in the reference table
    :return: DataFrame with referential integrity check results
    """
    df = session.table(table_name)
    ref_df = session.table(ref_table)
    df = df.join(ref_df, df[column] == ref_df[ref_column], "left_anti")
    return df

def check_timeliness(session: Session, table_name: str, column: str, min_date: str, max_date: str):
    """
    Check for timeliness of a date column.
    
    :param session: Snowflake session object
    :param table_name: Name of the table to check
    :param column: Date column to check for timeliness
    :param min_date: Minimum acceptable date
    :param max_date: Maximum acceptable date
    :return: DataFrame with timeliness check results
    """
    df = session.table(table_name)
    df = df.with_column(f"{column}_is_timely", (col(column) >= lit(min_date)) & (col(column) <= lit(max_date)))
    return df

def apply_quality_checks(session: Session):
    """
    Apply quality checks to call center tables.
    
    :param session: Snowflake session object
    :return: Dictionary of DataFrames with quality check results for each table
    """
    # Call_Details Table
    call_details_required_columns = ["call_id", "timestamp", "duration", "agent_id", "customer_id", "call_type", "call_outcome"]
    call_details_valid_call_types = ["inbound", "outbound"]
    call_details_valid_outcomes = ["resolved", "unresolved"]
    call_details_format_regex = r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"
    call_details_min_date = "2023-01-01"
    call_details_max_date = "2023-12-31"

    call_details_df = check_completeness(session, "Call_Details", call_details_required_columns)
    call_details_df = check_accuracy(session, "Call_Details", "call_type", call_details_valid_call_types)
    call_details_df = check_accuracy(session, "Call_Details", "call_outcome", call_details_valid_outcomes)
    call_details_df = check_consistency(session, "Call_Details", "timestamp", call_details_format_regex)
    call_details_df = check_uniqueness(session, "Call_Details", "call_id")
    call_details_df = check_referential_integrity(session, "Call_Details", "agent_id", "Agent_Details", "agent_id")
    call_details_df = check_referential_integrity(session, "Call_Details", "customer_id", "Customer_Details", "customer_id")
    call_details_df = check_timeliness(session, "Call_Details", "timestamp", call_details_min_date, call_details_max_date)

    # Agent_Details Table
    agent_details_required_columns = ["agent_id", "name", "department", "shift_details"]
    agent_details_valid_departments = ["sales", "support", "billing"]

    agent_details_df = check_completeness(session, "Agent_Details", agent_details_required_columns)
    agent_details_df = check_accuracy(session, "Agent_Details", "department", agent_details_valid_departments)
    agent_details_df = check_uniqueness(session, "Agent_Details", "agent_id")

    # Customer_Details Table
    customer_details_required_columns = ["customer_id", "name", "contact_info"]
    customer_details_format_regex = r"^\+?\d{10,15}$"

    customer_details_df = check_completeness(session, "Customer_Details", customer_details_required_columns)
    customer_details_df = check_consistency(session, "Customer_Details", "contact_info", customer_details_format_regex)
    customer_details_df = check_uniqueness(session, "Customer_Details", "customer_id")

    # Call_Queue Table
    call_queue_required_columns = ["queue_id", "call_id", "position", "wait_time"]
    call_queue_min_wait_time = 0
    call_queue_max_wait_time = 3600

    call_queue_df = check_completeness(session, "Call_Queue", call_queue_required_columns)
    call_queue_df = check_referential_integrity(session, "Call_Queue", "call_id", "Call_Details", "call_id")
    call_queue_df = call_queue_df.with_column("wait_time_is_valid", (col("wait_time") >= lit(call_queue_min_wait_time)) & (col("wait_time") <= lit(call_queue_max_wait_time)))

    # Call_Resolution Table
    call_resolution_required_columns = ["resolution_id", "call_id", "resolution_type", "resolution_time"]
    call_resolution_valid_types = ["technical", "billing", "general"]

    call_resolution_df = check_completeness(session, "Call_Resolution", call_resolution_required_columns)
    call_resolution_df = check_accuracy(session, "Call_Resolution", "resolution_type", call_resolution_valid_types)
    call_resolution_df = check_referential_integrity(session, "Call_Resolution", "call_id", "Call_Details", "call_id")

    return {
        "Call_Details": call_details_df,
        "Agent_Details": agent_details_df,
        "Customer_Details": customer_details_df,
        "Call_Queue": call_queue_df,
        "Call_Resolution": call_resolution_df
    }