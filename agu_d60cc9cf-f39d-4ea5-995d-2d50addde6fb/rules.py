from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, is_null, lit, current_date

def validate_call_center_data(session: Session, table_name: str):
    """
    Validates the data quality of the call_center table.

    Parameters:
    - session: Snowflake session object.
    - table_name: Name of the call_center table.

    Returns:
    - A dictionary containing DataFrames for each data quality check.
    """
    try:
        # Load the call_center table
        call_center_df = session.table(table_name)
        
        # Completeness: Ensure no critical data is missing
        completeness_check = call_center_df.filter(
            is_null(col("call_center_id")) |
            is_null(col("call_date")) |
            is_null(col("agent_id")) |
            is_null(col("customer_id")) |
            is_null(col("call_duration")) |
            is_null(col("call_type")) |
            is_null(col("call_result"))
        )
        
        # Accuracy: Validate call_type and call_result
        valid_call_types = ["inbound", "outbound"]
        valid_call_results = ["resolved", "unresolved"]
        accuracy_check = call_center_df.filter(
            ~col("call_type").isin(valid_call_types) |
            ~col("call_result").isin(valid_call_results)
        )
        
        # Consistency: Validate call_date format and call_duration
        consistency_check = call_center_df.filter(
            col("call_date").cast("date").is_null() |
            (col("call_duration") <= 0)
        )
        
        # Timeliness: Validate call_date is not in the future
        timeliness_check = call_center_df.filter(
            col("call_date") > lit(current_date())
        )
        
        # Validity: Check data types
        validity_check = call_center_df.filter(
            col("call_center_id").cast("int").is_null() |
            col("agent_id").cast("int").is_null() |
            col("customer_id").cast("int").is_null() |
            col("call_duration").cast("int").is_null()
        )
        
        # Uniqueness: Check unique call_center_id
        uniqueness_check = call_center_df.group_by("call_center_id").count().filter(col("count") > 1)
        
        # Integrity: Check valid references for agent_id and customer_id
        # Assuming there are related tables for agents and customers
        # This part is commented out as it requires additional context
        # integrity_check = call_center_df.filter(
        #     ~col("agent_id").isin(valid_agent_ids) |
        #     ~col("customer_id").isin(valid_customer_ids)
        # )
        
        # Return all checks
        return {
            "completeness_check": completeness_check,
            "accuracy_check": accuracy_check,
            "consistency_check": consistency_check,
            "timeliness_check": timeliness_check,
            "validity_check": validity_check,
            "uniqueness_check": uniqueness_check,
            # "integrity_check": integrity_check
        }
    
    except Exception as e:
        print(f"An error occurred during validation: {e}")
        return None