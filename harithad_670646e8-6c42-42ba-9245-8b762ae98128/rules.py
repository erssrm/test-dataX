from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, is_null, lit, when, countDistinct

def validate_call_details(session: Session):
    call_details = session.table("Call_Details")
    agent_details = session.table("Agent_Details")
    customer_details = session.table("Customer_Details")

    # Completeness: Ensure all required fields are filled in
    completeness_check = call_details.filter(
        is_null(col("call_id")) |
        is_null(col("timestamp")) |
        is_null(col("agent_id")) |
        is_null(col("customer_id")) |
        is_null(col("call_outcome"))
    )

    # Accuracy: Validate call_type and timestamp format
    valid_call_types = ["inbound", "outbound", "support", "sales"]
    accuracy_check = call_details.filter(
        ~col("call_type").isin(valid_call_types) |
        ~col("timestamp").rlike(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")
    )

    # Consistency: Ensure referential integrity using join
    consistency_check = call_details.join(agent_details, call_details["agent_id"] == agent_details["agent_id"], "left_anti").select(call_details["agent_id"]).union(
        call_details.join(customer_details, call_details["customer_id"] == customer_details["customer_id"], "left_anti").select(call_details["customer_id"])
    )

    # Timeliness: Check timestamp range
    timeliness_check = call_details.filter(
        col("timestamp") < lit("2020-01-01 00:00:00")  # Example range check
    )

    # Validity: Ensure call_duration is non-negative and call_outcome is valid
    valid_call_outcomes = ["completed", "missed", "transferred"]
    validity_check = call_details.filter(
        col("call_duration") < 0 |
        ~col("call_outcome").isin(valid_call_outcomes)
    )

    # Uniqueness: Ensure call_id is unique
    uniqueness_check = call_details.group_by("call_id").agg(countDistinct("call_id").alias("count")).filter(col("count") > 1)

    # Integrity: Ensure logical consistency
    integrity_check = call_details.filter(
        (col("call_outcome") == "transferred") & is_null(col("transfer_id"))
    )

    return {
        "completeness_check": completeness_check,
        "accuracy_check": accuracy_check,
        "consistency_check": consistency_check,
        "timeliness_check": timeliness_check,
        "validity_check": validity_check,
        "uniqueness_check": uniqueness_check,
        "integrity_check": integrity_check
    }