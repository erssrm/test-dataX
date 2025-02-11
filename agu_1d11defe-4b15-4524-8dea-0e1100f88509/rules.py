from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, is_null, lit, when

def validate_call_center_data(session: Session, call_center_df):
    # Completeness: Ensure all required fields are filled in
    completeness_check = call_center_df.filter(
        is_null(col("CALL_CENTER_ID")) |
        is_null(col("CALL_CENTER_NAME")) |
        is_null(col("LOCATION")) |
        is_null(col("OPENING_DATE")) |
        is_null(col("MANAGER_ID"))
    )

    # Accuracy: Validate LOCATION and date formats
    valid_locations = ["Location1", "Location2", "Location3"]  # Example list of valid locations
    accuracy_check = call_center_df.filter(
        ~col("LOCATION").isin(valid_locations) |
        ~col("OPENING_DATE").rlike(r"^\d{4}-\d{2}-\d{2}$") |
        (~is_null(col("CLOSING_DATE")) & ~col("CLOSING_DATE").rlike(r"^\d{4}-\d{2}-\d{2}$"))
    )

    # Consistency: Ensure referential integrity and consistent formats
    # Assuming a manager_df exists with MANAGER_IDs
    manager_df = session.table("manager")
    consistency_check = call_center_df.join(manager_df, call_center_df["MANAGER_ID"] == manager_df["MANAGER_ID"], "left_anti")

    # Timeliness: Ensure dates are logical
    timeliness_check = call_center_df.filter(
        col("OPENING_DATE") > lit("current_date()") |
        (~is_null(col("CLOSING_DATE")) & (col("CLOSING_DATE") < col("OPENING_DATE")))
    )

    # Validity: Check numeric fields
    validity_check = call_center_df.filter(
        (col("TOTAL_CALLS") < 0) |
        (col("SUCCESSFUL_CALLS") < 0) |
        (col("FAILED_CALLS") < 0) |
        (col("AVERAGE_WAIT_TIME") < 0)
    )

    # Uniqueness: Ensure CALL_CENTER_ID is unique
    uniqueness_check = call_center_df.group_by("CALL_CENTER_ID").count().filter(col("count") > 1)

    # Integrity: Logical consistency
    integrity_check = call_center_df.filter(
        (col("SUCCESSFUL_CALLS") + col("FAILED_CALLS")) > col("TOTAL_CALLS")
    )

    # Compliance: Placeholder for compliance checks
    compliance_check = call_center_df.filter(lit(False))  # No specific compliance checks implemented

    # Combine all checks
    all_checks = completeness_check.union_all(accuracy_check).union_all(consistency_check).union_all(
        timeliness_check).union_all(validity_check).union_all(uniqueness_check).union_all(integrity_check).union_all(
        compliance_check)

    return all_checks