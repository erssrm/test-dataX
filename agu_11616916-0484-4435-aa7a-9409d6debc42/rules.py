from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, regexp, reduce

def validate_call_center_data(session: Session, call_center_df):
    # Completeness: Ensure all required fields are filled in
    completeness_checks = [
        col("call_center_id").is_not_null(),
        col("call_center_name").is_not_null(),
        col("open_date").is_not_null(),
        col("city").is_not_null(),
        col("state").is_not_null(),
        col("country").is_not_null(),
        col("manager").is_not_null(),
        col("phone_number").is_not_null()
    ]

    # Accuracy: Valid Values and Format Compliance
    accuracy_checks = [
        col("country").isin(["USA", "CAN", "MEX"]),  # Example country codes
        regexp(col("phone_number"), r"^\(\d{3}\) \d{3}-\d{4}$")  # Example phone number format
    ]

    # Consistency: Uniformity and Referential Integrity
    consistency_checks = [
        col("open_date").cast("date").is_not_null(),
        col("close_date").cast("date").is_not_null()
    ]

    # Timeliness: Current Data
    timeliness_checks = [
        col("open_date") <= col("close_date")
    ]

    # Validity: Data Type Check and Range Checks
    validity_checks = [
        col("call_center_id").cast("int").is_not_null(),
        col("open_date").cast("date").is_not_null(),
        col("close_date").cast("date").is_not_null()
    ]

    # Uniqueness: Primary Key Uniqueness
    uniqueness_checks = [
        call_center_df.group_by("call_center_id").count().filter(col("count") > 1).count() == 0
    ]

    # Integrity: Logical Consistency
    integrity_checks = [
        col("close_date") >= col("open_date")
    ]

    # Combine all checks
    all_checks = completeness_checks + accuracy_checks + consistency_checks + timeliness_checks + validity_checks + uniqueness_checks + integrity_checks

    # Filter the DataFrame based on the checks
    valid_call_center_df = call_center_df.filter(
        reduce(lambda x, y: x & y, all_checks)
    )

    return valid_call_center_df

# Example usage
# session = Session.builder.configs(...).create()
# call_center_df = session.table("call_center")
# valid_call_center_df = validate_call_center_data(session, call_center_df)