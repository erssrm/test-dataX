from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, is_null, lit, when, count, expr

def validate_call_center_data(session: Session, table_name: str):
    # Load the table
    df = session.table(table_name)

    # Completeness: Ensure mandatory fields are not null
    completeness_checks = [
        df.filter(is_null(col("call_id"))).count() == 0,
        df.filter(is_null(col("agent_id"))).count() == 0,
        df.filter(is_null(col("customer_id"))).count() == 0,
        df.filter(is_null(col("call_start_time"))).count() == 0,
        df.filter(is_null(col("call_end_time"))).count() == 0
    ]

    # Accuracy: Valid values and format compliance
    valid_call_types = ["inquiry", "complaint", "support"]
    valid_call_statuses = ["completed", "dropped", "in-progress"]
    accuracy_checks = [
        df.filter(~col("call_type").isin(valid_call_types)).count() == 0,
        df.filter(~col("call_status").isin(valid_call_statuses)).count() == 0
    ]

    # Consistency: Referential integrity and uniformity
    # Assuming agent and customer tables exist for referential integrity checks
    agent_ids_df = session.table("agents").select("agent_id")
    customer_ids_df = session.table("customers").select("customer_id")
    consistency_checks = [
        df.join(agent_ids_df, df["agent_id"] == agent_ids_df["agent_id"], "left_anti").count() == 0,
        df.join(customer_ids_df, df["customer_id"] == customer_ids_df["customer_id"], "left_anti").count() == 0
    ]

    # Timeliness: Current data and timestamp validation
    timeliness_checks = [
        df.filter(col("call_start_time") > col("call_end_time")).count() == 0
    ]

    # Validity: Data type and range checks
    validity_checks = [
        df.filter(col("call_duration") < 0).count() == 0
    ]

    # Uniqueness: Primary key uniqueness
    uniqueness_checks = [
        df.group_by("call_id").agg(count("*").alias("count")).filter(col("count") > 1).count() == 0
    ]

    # Integrity: Logical consistency
    integrity_checks = [
        df.filter(col("call_duration") != expr("datediff('second', call_start_time, call_end_time)")).count() == 0
    ]

    # Consistency with Business Rules
    business_rule_checks = [
        df.filter((col("call_status") == "completed") & (col("call_duration") <= 0)).count() == 0
    ]

    # Combine all checks
    all_checks = (completeness_checks + accuracy_checks + consistency_checks +
                  timeliness_checks + validity_checks + uniqueness_checks +
                  integrity_checks + business_rule_checks)

    # Return True if all checks pass
    return all(all_checks)