from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, is_null, lit, when

def validate_call_center_data(session: Session, call_center_df):
    # Completeness: Ensure all required fields are filled in
    completeness_check = call_center_df.filter(
        is_null(col("call_id")) |
        is_null(col("agent_id")) |
        is_null(col("call_start_time")) |
        is_null(col("call_end_time"))
    )

    # Accuracy: Check valid values and format compliance
    valid_values_check = call_center_df.filter(
        ~col("call_type").isin(["inbound", "outbound", "internal"])
    )
    format_compliance_check = call_center_df.filter(
        ~col("phone_number").rlike(r"^\+?[1-9]\d{1,14}$")
    )

    # Consistency: Check referential integrity
    referential_integrity_check = call_center_df.join(
        session.table("agent"), call_center_df["agent_id"] == col("agent.agent_id"), "left_anti"
    )

    # Timeliness: Check timestamp validation
    timestamp_validation_check = call_center_df.filter(
        col("call_start_time") > col("call_end_time")
    )

    # Validity: Check data types and range
    range_check = call_center_df.filter(
        (col("call_duration") < 0) | (col("call_duration") > 86400)
    )

    # Uniqueness: Check primary key uniqueness
    uniqueness_check = call_center_df.group_by("call_id").count().filter(col("count") > 1)

    # Integrity: Logical consistency
    logical_consistency_check = call_center_df.filter(
        col("call_start_time") > col("call_end_time")
    )

    # Consistency with Business Rules: Business logic validation
    business_logic_check = call_center_df.filter(
        (col("call_type") == "internal") & (col("external_number").is_not_null())
    )

    # Combine all checks
    quality_issues_df = completeness_check.union_all(valid_values_check) \
        .union_all(format_compliance_check) \
        .union_all(referential_integrity_check) \
        .union_all(timestamp_validation_check) \
        .union_all(range_check) \
        .union_all(uniqueness_check) \
        .union_all(logical_consistency_check) \
        .union_all(business_logic_check)

    return quality_issues_df

# Example usage
# session = Session.builder.configs(...).create()
# call_center_df = session.table("call_center")
# quality_issues_df = validate_call_center_data(session, call_center_df)
# quality_issues_df.show()