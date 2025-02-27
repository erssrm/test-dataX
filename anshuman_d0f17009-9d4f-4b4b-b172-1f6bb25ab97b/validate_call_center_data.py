from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit, when, is_null

def validate_call_center_data(session: Session, table_name: str):
    df = session.table(table_name)

    # Completeness: Ensure all required fields are filled in
    completeness_check = df.filter(
        is_null(col("Call_ID")) |
        is_null(col("Agent_ID")) |
        is_null(col("Customer_ID")) |
        is_null(col("Call_Start_Time")) |
        is_null(col("Call_End_Time")) |
        is_null(col("Call_Type")) |
        is_null(col("Call_Status")) |
        is_null(col("Resolution_Status"))
    )

    # Accuracy: Ensure Call_Type and Call_Status match valid value sets
    valid_call_types = ["inquiry", "complaint", "support"]
    valid_call_statuses = ["completed", "dropped"]

    accuracy_check = df.filter(
        ~col("Call_Type").isin(valid_call_types) |
        ~col("Call_Status").isin(valid_call_statuses)
    )

    # Consistency: Ensure Call_End_Time is greater than Call_Start_Time
    consistency_check = df.filter(
        col("Call_End_Time") <= col("Call_Start_Time")
    )

    # Timeliness: Ensure Call_Start_Time and Call_End_Time are not in the future
    current_time = session.sql("SELECT CURRENT_TIMESTAMP()").collect()[0][0]

    timeliness_check = df.filter(
        col("Call_Start_Time") > lit(current_time) |
        col("Call_End_Time") > lit(current_time)
    )

    # Validity: Ensure data types and ranges
    validity_check = df.filter(
        col("Call_ID").cast("int").is_null() |
        col("Agent_ID").cast("int").is_null() |
        col("Customer_ID").cast("int").is_null()
    )

    # Uniqueness: Ensure Call_ID is unique
    uniqueness_check = df.group_by("Call_ID").count().filter(col("count") > 1)

    # Integrity: Ensure Resolution_Status is valid based on Call_Status
    integrity_check = df.filter(
        (col("Call_Status") == "completed") & (col("Resolution_Status") == "unresolved")
    )

    # Consistency with Business Rules: Ensure Call_Duration is correct
    business_rule_check = df.filter(
        col("Call_Duration") != (col("Call_End_Time") - col("Call_Start_Time"))
    )

    return {
        "completeness_issues": completeness_check,
        "accuracy_issues": accuracy_check,
        "consistency_issues": consistency_check,
        "timeliness_issues": timeliness_check,
        "validity_issues": validity_check,
        "uniqueness_issues": uniqueness_check,
        "integrity_issues": integrity_check,
        "business_rule_issues": business_rule_check
    }