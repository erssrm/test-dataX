from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, is_null, lit, when

def apply_quality_rules(session: Session, table_name: str, valid_location_codes=None, max_call_date="2023-12-31"):
    """
    Apply quality rules to the specified table.

    Parameters:
    - session: Snowflake session object.
    - table_name: Name of the table to apply quality rules.
    - valid_location_codes: List of valid location codes.
    - max_call_date: Maximum allowed call date for timeliness check.

    Returns:
    - DataFrame containing rows with quality issues.
    """
    if valid_location_codes is None:
        valid_location_codes = ["LOC1", "LOC2", "LOC3"]

    # Load the call_center table
    call_center_df = session.table(table_name)

    # Check if required columns exist
    required_columns = ["required_field_1", "required_field_2", "location_code", "phone_number", "call_date", "call_duration", "primary_key", "end_date", "start_date"]
    for column in required_columns:
        if column not in call_center_df.columns:
            raise ValueError(f"Column {column} does not exist in the table.")

    # Completeness: Ensure all required fields are filled in
    completeness_check = call_center_df.filter(
        is_null(col("required_field_1")) |
        is_null(col("required_field_2"))
    )

    # Accuracy: Ensure data entries match valid value sets
    valid_values_check = call_center_df.filter(
        ~col("location_code").isin(valid_location_codes)
    )

    # Consistency: Ensure consistent use of data formats
    consistency_check = call_center_df.filter(
        col("phone_number").rlike(r"^\d{3}-\d{3}-\d{4}$") == False
    )

    # Timeliness: Ensure dates and times are logical
    timeliness_check = call_center_df.filter(
        col("call_date") > lit(max_call_date)
    )

    # Validity: Ensure data types are as expected
    validity_check = call_center_df.filter(
        col("call_duration").cast("int").is_null()
    )

    # Uniqueness: Ensure no duplicate primary keys
    uniqueness_check = call_center_df.group_by("primary_key").count().filter(col("count") > 1)

    # Integrity: Ensure logical consistency
    integrity_check = call_center_df.filter(
        col("end_date") < col("start_date")
    )

    # Combine all checks
    quality_issues_df = completeness_check.union_all(valid_values_check) \
                                           .union_all(consistency_check) \
                                           .union_all(timeliness_check) \
                                           .union_all(validity_check) \
                                           .union_all(uniqueness_check) \
                                           .union_all(integrity_check)

    # Return the DataFrame with quality issues
    return quality_issues_df

# Example usage
# session = Session.builder.configs(...).create()
# quality_issues = apply_quality_rules(session, "call_center")
# quality_issues.show()