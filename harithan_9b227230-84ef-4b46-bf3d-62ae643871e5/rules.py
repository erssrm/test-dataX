from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, current_timestamp, lit, countDistinct

def validate_call_center_data(session: Session, table_name: str):
    df = session.table(table_name)

    # Completeness: Ensure all required fields are filled in
    completeness_checks = [
        df.filter(col("call_id").is_null()).count() == 0,
        df.filter(col("agent_id").is_null()).count() == 0,
        df.filter(col("customer_id").is_null()).count() == 0,
        df.filter(col("call_start_time").is_null()).count() == 0,
        df.filter(col("call_end_time").is_null()).count() == 0,
        df.filter(col("call_type").is_null()).count() == 0,
        df.filter(col("call_status").is_null()).count() == 0
    ]

    # Accuracy: Valid Values
    accuracy_checks = [
        df.filter(~col("call_type").isin("inbound", "outbound")).count() == 0,
        df.filter(~col("call_status").isin("completed", "dropped")).count() == 0
    ]

    # Consistency: call_duration should match the difference between call_end_time and call_start_time
    consistency_checks = [
        df.filter((col("call_end_time") - col("call_start_time")) != col("call_duration")).count() == 0
    ]

    # Timeliness: call_start_time and call_end_time should not be in the future
    timeliness_checks = [
        df.filter(col("call_start_time") > current_timestamp()).count() == 0,
        df.filter(col("call_end_time") > current_timestamp()).count() == 0
    ]

    # Validity: Data Type Check
    validity_checks = [
        df.filter(col("call_duration") <= 0).count() == 0
    ]

    # Uniqueness: call_id should be unique
    uniqueness_checks = [
        df.select(countDistinct("call_id")).collect()[0][0] == df.count()
    ]

    # Integrity: agent_id and customer_id should reference valid records
    # Assuming there are related tables 'agents' and 'customers'
    integrity_checks = [
        df.join(session.table("agents"), "agent_id", "left_anti").count() == 0,
        df.join(session.table("customers"), "customer_id", "left_anti").count() == 0
    ]

    # Consistency with Business Rules: call_end_time should not precede call_start_time
    business_rule_checks = [
        df.filter(col("call_end_time") < col("call_start_time")).count() == 0
    ]

    # Combine all checks
    all_checks = completeness_checks + accuracy_checks + consistency_checks + timeliness_checks + validity_checks + uniqueness_checks + integrity_checks + business_rule_checks

    # Collect errors
    errors = [not check for check in all_checks]

    return errors

# Example usage
# session = Session.builder.configs(connection_parameters).create()
# errors = validate_call_center_data(session, "call_center_data")
# print(errors)