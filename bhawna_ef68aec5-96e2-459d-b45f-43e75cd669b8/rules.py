from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, is_null, lit, when

def validate_call_details(session: Session):
    call_details = session.table("Call_Details")
    
    # Completeness: Ensure all required fields are filled in
    completeness_check = call_details.filter(
        is_null(col("call_id")) |
        is_null(col("timestamp")) |
        is_null(col("duration")) |
        is_null(col("agent_id")) |
        is_null(col("customer_id")) |
        is_null(col("call_type")) |
        is_null(col("call_outcome"))
    )
    
    # Accuracy: Validate call types and outcomes
    valid_call_types = ["inbound", "outbound"]
    valid_call_outcomes = ["completed", "missed", "voicemail"]
    accuracy_check = call_details.filter(
        ~col("call_type").isin(valid_call_types) |
        ~col("call_outcome").isin(valid_call_outcomes)
    )
    
    # Uniqueness: Ensure call ID is unique
    uniqueness_check = call_details.group_by("call_id").count().filter(col("count") > 1)
    
    return completeness_check, accuracy_check, uniqueness_check

def validate_agent_details(session: Session):
    agent_details = session.table("Agent_Details")
    
    # Completeness: Ensure all required fields are filled in
    completeness_check = agent_details.filter(
        is_null(col("agent_id")) |
        is_null(col("name")) |
        is_null(col("department")) |
        is_null(col("shift_details"))
    )
    
    # Uniqueness: Ensure agent ID is unique
    uniqueness_check = agent_details.group_by("agent_id").count().filter(col("count") > 1)
    
    return completeness_check, uniqueness_check

def validate_customer_details(session: Session):
    customer_details = session.table("Customer_Details")
    
    # Completeness: Ensure all required fields are filled in
    completeness_check = customer_details.filter(
        is_null(col("customer_id")) |
        is_null(col("name")) |
        is_null(col("contact_info"))
    )
    
    # Uniqueness: Ensure customer ID is unique
    uniqueness_check = customer_details.group_by("customer_id").count().filter(col("count") > 1)
    
    return completeness_check, uniqueness_check

def validate_call_queue(session: Session):
    call_queue = session.table("Call_Queue")
    
    # Completeness: Ensure all required fields are filled in
    completeness_check = call_queue.filter(
        is_null(col("queue_id")) |
        is_null(col("call_id")) |
        is_null(col("position")) |
        is_null(col("wait_time"))
    )
    
    # Referential Integrity: Ensure call IDs exist in Call_Details
    referential_integrity_check = call_queue.join(
        session.table("Call_Details"),
        call_queue["call_id"] == col("call_id"),
        "left_anti"
    )
    
    return completeness_check, referential_integrity_check

def validate_call_resolution(session: Session):
    call_resolution = session.table("Call_Resolution")
    
    # Completeness: Ensure all required fields are filled in
    completeness_check = call_resolution.filter(
        is_null(col("resolution_id")) |
        is_null(col("call_id")) |
        is_null(col("resolution_type")) |
        is_null(col("resolution_time"))
    )
    
    # Referential Integrity: Ensure call IDs exist in Call_Details
    referential_integrity_check = call_resolution.join(
        session.table("Call_Details"),
        call_resolution["call_id"] == col("call_id"),
        "left_anti"
    )
    
    return completeness_check, referential_integrity_check