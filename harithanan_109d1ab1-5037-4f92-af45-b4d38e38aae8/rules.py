from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit, when, is_null

# Constants for valid values and date limits
VALID_CALL_TYPES = ["inbound", "outbound"]
VALID_CALL_STATUSES = ["completed", "missed"]
VALID_AGENT_STATUSES = ["active", "inactive"]
VALID_ACCOUNT_STATUSES = ["active", "inactive"]
VALID_RESOLUTION_STATUSES = ["resolved", "unresolved"]
DATE_LIMIT = "2023-12-31"

def validate_completeness(table, required_fields):
    return table.filter(
        when(is_null(col(field)), True).otherwise(False) for field in required_fields
    )

def validate_uniqueness(table, unique_field):
    return table.group_by(unique_field).count().filter(col("count") > 1)

def validate_call_details(session: Session):
    call_details = session.table("Call_Details")

    # Completeness
    completeness_check = validate_completeness(call_details, [
        "Call_ID", "Caller_ID", "Receiver_ID", "Call_Start_Time", "Call_End_Time", "Call_Duration"
    ])

    # Accuracy
    accuracy_check = call_details.filter(
        ~col("Call_Type").isin(VALID_CALL_TYPES) |
        ~col("Call_Status").isin(VALID_CALL_STATUSES)
    )

    # Consistency
    consistency_check = call_details.filter(
        col("Call_Start_Time") >= col("Call_End_Time") |
        col("Call_Duration") != (col("Call_End_Time") - col("Call_Start_Time"))
    )

    # Uniqueness
    uniqueness_check = validate_uniqueness(call_details, "Call_ID")

    # Timeliness
    timeliness_check = call_details.filter(
        col("Call_Start_Time") > lit(DATE_LIMIT) |
        col("Call_End_Time") > lit(DATE_LIMIT)
    )

    return {
        "completeness": completeness_check,
        "accuracy": accuracy_check,
        "consistency": consistency_check,
        "uniqueness": uniqueness_check,
        "timeliness": timeliness_check
    }

def validate_agent_details(session: Session):
    agent_details = session.table("Agent_Details")

    # Completeness
    completeness_check = validate_completeness(agent_details, [
        "Agent_ID", "Agent_Name", "Agent_Role"
    ])

    # Accuracy
    accuracy_check = agent_details.filter(
        ~col("Agent_Status").isin(VALID_AGENT_STATUSES)
    )

    # Uniqueness
    uniqueness_check = validate_uniqueness(agent_details, "Agent_ID")

    # Timeliness
    timeliness_check = agent_details.filter(
        col("Hire_Date") > lit(DATE_LIMIT)
    )

    return {
        "completeness": completeness_check,
        "accuracy": accuracy_check,
        "uniqueness": uniqueness_check,
        "timeliness": timeliness_check
    }

def validate_customer_details(session: Session):
    customer_details = session.table("Customer_Details")

    # Completeness
    completeness_check = validate_completeness(customer_details, [
        "Customer_ID", "Customer_Name", "Contact_Number"
    ])

    # Accuracy
    accuracy_check = customer_details.filter(
        ~col("Email_Address").rlike(r"^[\w\.-]+@[\w\.-]+\.\w+$") |
        ~col("Account_Status").isin(VALID_ACCOUNT_STATUSES)
    )

    # Uniqueness
    uniqueness_check = validate_uniqueness(customer_details, "Customer_ID")

    return {
        "completeness": completeness_check,
        "accuracy": accuracy_check,
        "uniqueness": uniqueness_check
    }

def validate_call_queue(session: Session):
    call_queue = session.table("Call_Queue")

    # Completeness
    completeness_check = validate_completeness(call_queue, [
        "Queue_ID", "Call_ID", "Queue_Start_Time"
    ])

    # Consistency
    consistency_check = call_queue.filter(
        col("Queue_Start_Time") >= col("Queue_End_Time") |
        col("Queue_Duration") != (col("Queue_End_Time") - col("Queue_Start_Time"))
    )

    # Uniqueness
    uniqueness_check = validate_uniqueness(call_queue, "Queue_ID")

    return {
        "completeness": completeness_check,
        "consistency": consistency_check,
        "uniqueness": uniqueness_check
    }

def validate_call_resolution(session: Session):
    call_resolution = session.table("Call_Resolution")

    # Completeness
    completeness_check = validate_completeness(call_resolution, [
        "Resolution_ID", "Call_ID", "Resolution_Status"
    ])

    # Accuracy
    accuracy_check = call_resolution.filter(
        ~col("Resolution_Status").isin(VALID_RESOLUTION_STATUSES)
    )

    # Uniqueness
    uniqueness_check = validate_uniqueness(call_resolution, "Resolution_ID")

    return {
        "completeness": completeness_check,
        "accuracy": accuracy_check,
        "uniqueness": uniqueness_check
    }