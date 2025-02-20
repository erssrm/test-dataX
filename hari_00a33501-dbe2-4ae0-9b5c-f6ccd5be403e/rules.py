from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, is_null, lit, when, countDistinct

def validate_call_center_data(session: Session, table_name: str):
    # Load the table
    df = session.table(table_name)

    # Completeness: Ensure all required fields are filled in
    completeness_checks = [
        df.filter(is_null(col("call_id"))).count() == 0,
        df.filter(is_null(col("agent_id"))).count() == 0,
        df.filter(is_null(col("customer_id"))).count() == 0,
        df.filter(is_null(col("call_start_time"))).count() == 0,
        df.filter(is_null(col("call_end_time"))).count() == 0,
        df.filter(is_null(col("call_type"))).count() == 0,
        df.filter(is_null(col("resolution_status"))).count() == 0
    ]

    # Accuracy: Validate call_type and resolution_status
    valid_call_types = ["inquiry", "complaint", "support"]
    valid_resolution_statuses = ["resolved", "unresolved"]
    accuracy_checks = [
        df.filter(~col("call_type").isin(valid_call_types)).count() == 0,
        df.filter(~col("resolution_status").isin(valid_resolution_statuses)).count() == 0
    ]

    # Consistency: Check call_duration consistency
    consistency_checks = [
        df.filter((col("call_duration") != (col("call_end_time") - col("call_start_time")))).count() == 0
    ]

    # Timeliness: Check logical time ranges
    timeliness_checks = [
        df.filter(col("call_start_time") > col("call_end_time")).count() == 0
    ]

    # Validity: Check data types and positive call_duration
    validity_checks = [
        df.filter(col("call_duration") < 0).count() == 0
    ]

    # Uniqueness: Ensure call_id is unique
    uniqueness_checks = [
        df.select(countDistinct("call_id")).collect()[0][0] == df.count()
    ]

    # Integrity: Check resolution_status and customer_feedback
    integrity_checks = [
        df.filter((col("resolution_status") == "resolved") & is_null(col("customer_feedback"))).count() == 0
    ]

    # Combine all checks
    all_checks = completeness_checks + accuracy_checks + consistency_checks + timeliness_checks + validity_checks + uniqueness_checks + integrity_checks

    # Return True if all checks pass
    return all(all_checks)