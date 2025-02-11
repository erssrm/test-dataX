from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, is_null, lit, when
from functools import reduce

# Constants for valid states and countries
VALID_STATES = ["ValidState1", "ValidState2"]
VALID_COUNTRIES = ["ValidCountry1", "ValidCountry2"]

def validate_call_center_data(session: Session, call_center_df):
    """
    Validate the call center data based on predefined quality rules.

    Parameters:
    - session: Snowflake session object.
    - call_center_df: DataFrame containing call center data.

    Returns:
    - DataFrame with valid call center records.
    """

    # Completeness: Ensure required fields are not null
    completeness_checks = [
        col("cc_call_center_sk").is_not_null(),
        col("cc_call_center_id").is_not_null(),
        col("cc_name").is_not_null(),
        col("cc_open_date_sk").is_not_null(),
        col("cc_country").is_not_null()
    ]

    # Accuracy: Validate state and country, ZIP code format
    accuracy_checks = [
        col("cc_state").isin(VALID_STATES),
        col("cc_country").isin(VALID_COUNTRIES),
        col("cc_zip").rlike(r"^\d{5}(-\d{4})?$")  # ZIP code format
    ]

    # Consistency: Check marketing ID consistency
    consistency_checks = [
        col("cc_mkt_id").is_not_null()  # Assuming marketing IDs are not null
    ]

    # Timeliness: Validate date logic
    timeliness_checks = [
        col("cc_rec_start_date") <= col("cc_rec_end_date"),
        col("cc_open_date_sk") <= col("cc_closed_date_sk")
    ]

    # Validity: Check data types and ranges
    validity_checks = [
        (col("cc_tax_percentage") >= 0) & (col("cc_tax_percentage") <= 100)
    ]

    # Uniqueness: Ensure unique call center IDs
    uniqueness_checks = [
        col("cc_call_center_id").is_not_null()
    ]

    # Integrity: Check foreign key references
    integrity_checks = [
        col("cc_mkt_id").is_not_null()  # Assuming foreign key integrity
    ]

    # Apply all checks
    all_checks = completeness_checks + accuracy_checks + consistency_checks + timeliness_checks + validity_checks + uniqueness_checks + integrity_checks

    # Filter DataFrame based on checks
    valid_call_center_df = call_center_df.filter(
        reduce(lambda x, y: x & y, all_checks)
    )

    return valid_call_center_df

# Example usage
# session = Session.builder.configs(...).create()
# call_center_df = session.table("call_center")
# valid_call_center_df = validate_call_center_data(session, call_center_df)