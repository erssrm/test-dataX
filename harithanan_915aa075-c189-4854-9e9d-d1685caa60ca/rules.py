from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, is_null, lit, when

def check_nulls(df, columns):
    return df.filter(
        reduce(lambda acc, col_name: acc | is_null(col(col_name)), columns, lit(False))
    )

def validate_call_details(session: Session):
    call_details = session.table("Call_Details")
    
    # Completeness: Ensure all required fields are filled in
    completeness_check = check_nulls(call_details, ["call_id", "timestamp", "duration", "agent_id", "customer_id", "call_type", "call_outcome"])
    
    # Accuracy: Validate call types and outcomes
    valid_call_types = ["inbound", "outbound"]
    valid_call_outcomes = ["completed", "missed", "voicemail"]
    accuracy_check = call_details.filter(
        ~col("call_type").isin(valid_call_types) |
        ~col("call_outcome").isin(valid_call_outcomes)
    )
    
    # Consistency: Ensure referential integrity
    agent_details = session.table("Agent_Details")
    customer_details = session.table("Customer_Details")
    consistency_check = call_details.filter(
        ~col("agent_id").isin(agent_details.select("agent_id")) |
        ~col("customer_id").isin(customer_details.select("customer_id"))
    )
    
    # Uniqueness: Ensure call IDs are unique
    uniqueness_check = call_details.group_by("call_id").count().filter(col("count") > 1)
    
    return completeness_check, accuracy_check, consistency_check, uniqueness_check

def validate_agent_details(session: Session):
    agent_details = session.table("Agent_Details")
    
    # Completeness: Ensure all required fields are filled in
    completeness_check = check_nulls(agent_details, ["agent_id", "name", "department", "shift_details"])
    
    # Accuracy: Validate department names
    valid_departments = ["sales", "support", "billing"]
    accuracy_check = agent_details.filter(
        ~col("department").isin(valid_departments)
    )
    
    # Uniqueness: Ensure agent IDs are unique
    uniqueness_check = agent_details.group_by("agent_id").count().filter(col("count") > 1)
    
    return completeness_check, accuracy_check, uniqueness_check

def validate_customer_details(session: Session):
    customer_details = session.table("Customer_Details")
    
    # Completeness: Ensure all required fields are filled in
    completeness_check = check_nulls(customer_details, ["customer_id", "name", "contact_info"])
    
    # Uniqueness: Ensure customer IDs are unique
    uniqueness_check = customer_details.group_by("customer_id").count().filter(col("count") > 1)
    
    return completeness_check, uniqueness_check

def validate_call_queue(session: Session):
    call_queue = session.table("Call_Queue")
    call_details = session.table("Call_Details")
    
    # Completeness: Ensure all required fields are filled in
    completeness_check = check_nulls(call_queue, ["queue_id", "call_id", "position", "estimated_wait_time"])
    
    # Referential Integrity: Ensure call IDs exist in Call_Details
    referential_integrity_check = call_queue.filter(
        ~col("call_id").isin(call_details.select("call_id"))
    )
    
    return completeness_check, referential_integrity_check

def validate_call_resolution(session: Session):
    call_resolution = session.table("Call_Resolution")
    call_details = session.table("Call_Details")
    
    # Completeness: Ensure all required fields are filled in
    completeness_check = check_nulls(call_resolution, ["resolution_id", "call_id", "resolution_type", "resolution_time"])
    
    # Referential Integrity: Ensure call IDs exist in Call_Details
    referential_integrity_check = call_resolution.filter(
        ~col("call_id").isin(call_details.select("call_id"))
    )
    
    return completeness_check, referential_integrity_check