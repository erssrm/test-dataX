from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, count, lit, when

def check_uniqueness(df, column_name):
    return df.group_by(column_name).agg(count("*").alias("count"))["count"] == 1

def validate_table(df, required_columns, unique_column):
    try:
        is_valid = True
        for column in required_columns:
            is_valid &= col(column).is_not_null()
        is_valid &= check_uniqueness(df, unique_column)
        return df.with_column("is_valid", is_valid.cast("boolean"))
    except Exception as e:
        print(f"Error validating table: {e}")
        return df.with_column("is_valid", lit(False))

def validate_call_records(session: Session):
    df = session.table("call_records")
    required_columns = ["call_id", "timestamp", "duration", "agent_id", "customer_id", "call_type", "call_outcome"]
    return validate_table(df, required_columns, "call_id")

def validate_agent_information(session: Session):
    df = session.table("agent_information")
    required_columns = ["agent_id", "name", "shift_details"]
    return validate_table(df, required_columns, "agent_id")

def validate_customer_information(session: Session):
    df = session.table("customer_information")
    required_columns = ["customer_id", "name", "contact_information"]
    return validate_table(df, required_columns, "customer_id")

def validate_call_queue(session: Session):
    df = session.table("call_queue")
    required_columns = ["queue_id", "call_id", "position_in_queue", "wait_time"]
    return validate_table(df, required_columns, "queue_id")

def validate_call_dispositions(session: Session):
    df = session.table("call_dispositions")
    required_columns = ["disposition_id", "call_outcome"]
    return validate_table(df, required_columns, "disposition_id")

def validate_slas(session: Session):
    df = session.table("slas")
    required_columns = ["sla_id", "response_times", "resolution_times"]
    return validate_table(df, required_columns, "sla_id")

def validate_feedback_surveys(session: Session):
    df = session.table("feedback_surveys")
    required_columns = ["feedback_id", "customer_id", "survey_results"]
    return validate_table(df, required_columns, "feedback_id")