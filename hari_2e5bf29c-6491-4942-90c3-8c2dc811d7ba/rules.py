from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, is_null, lit, when

def check_completeness(df, columns):
    return df.select(
        [is_null(col(column)).alias(f"{column}_is_null") for column in columns]
    ).filter(
        any(col(f"{column}_is_null") for column in columns)
    )

def check_uniqueness(df, column):
    return df.group_by(column).count().filter(col("count") > 1)

def check_valid_values(df, column, valid_values):
    return df.filter(~col(column).isin(valid_values))

def check_format(df, column, regex):
    return df.filter(~col(column).rlike(regex))

def check_completeness_call_details(session: Session):
    df = session.table("Call_Details")
    return check_completeness(df, ["call_id", "timestamp", "duration", "agent_id", "customer_id", "call_outcome"])

def check_accuracy_call_details(session: Session):
    df = session.table("Call_Details")
    valid_outcomes = ["Completed", "Missed", "Voicemail"]
    return check_valid_values(df, "call_outcome", valid_outcomes).union(
        check_format(df, "timestamp", r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")
    )

def check_uniqueness_call_details(session: Session):
    df = session.table("Call_Details")
    return check_uniqueness(df, "call_id")

def check_completeness_agent_details(session: Session):
    df = session.table("Agent_Details")
    return check_completeness(df, ["agent_id", "name", "department", "shift_timings"])

def check_uniqueness_agent_details(session: Session):
    df = session.table("Agent_Details")
    return check_uniqueness(df, "agent_id")

def check_completeness_customer_details(session: Session):
    df = session.table("Customer_Details")
    return check_completeness(df, ["customer_id", "name", "contact_info"])

def check_uniqueness_customer_details(session: Session):
    df = session.table("Customer_Details")
    return check_uniqueness(df, "customer_id")

def check_completeness_call_queue(session: Session):
    df = session.table("Call_Queue")
    return check_completeness(df, ["queue_id", "call_id", "position_in_queue", "wait_time"])

def check_completeness_call_resolution(session: Session):
    df = session.table("Call_Resolution")
    return check_completeness(df, ["resolution_id", "call_id", "resolution_status", "resolution_time"])

def check_accuracy_call_resolution(session: Session):
    df = session.table("Call_Resolution")
    valid_statuses = ["Resolved", "Unresolved"]
    return check_valid_values(df, "resolution_status", valid_statuses)