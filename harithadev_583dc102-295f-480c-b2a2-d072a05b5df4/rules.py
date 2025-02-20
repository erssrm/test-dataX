from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit, is_null, count

def validate_call_center_data(session: Session, table_name: str):
    df = session.table(table_name)

    # Completeness: Ensure all required fields are filled in
    completeness_checks = [
        df[col_name].is_not_null() for col_name in [
            'call_id', 'agent_id', 'customer_id', 'call_start_time', 
            'call_end_time', 'call_duration', 'call_type', 'call_resolution'
        ]
    ]
    completeness_check = df.select(*completeness_checks).filter(~col('completeness')).count() == 0

    # Accuracy: Validate call_type and call_resolution
    valid_call_types = ['inquiry', 'complaint', 'support']
    valid_call_resolutions = ['resolved', 'escalated']
    accuracy_checks = [
        df['call_type'].isin(valid_call_types),
        df['call_resolution'].isin(valid_call_resolutions)
    ]
    accuracy_check = df.select(*accuracy_checks).filter(~col('accuracy')).count() == 0

    # Consistency: Check timestamp format and call_duration
    consistency_checks = [
        df['call_start_time'].cast('timestamp').is_not_null(),
        df['call_end_time'].cast('timestamp').is_not_null(),
        (df['call_duration'] == (df['call_end_time'] - df['call_start_time']).cast('int'))
    ]
    consistency_check = df.select(*consistency_checks).filter(~col('consistency')).count() == 0

    # Timeliness: Check logical time ranges
    timeliness_checks = [
        df['call_start_time'] <= df['call_end_time'],
        df['call_start_time'] <= lit('current_timestamp()')
    ]
    timeliness_check = df.select(*timeliness_checks).filter(~col('timeliness')).count() == 0

    # Validity: Check data types and ranges
    validity_checks = [
        df['call_id'].cast('int').is_not_null(),
        df['agent_id'].cast('int').is_not_null(),
        df['customer_id'].cast('int').is_not_null(),
        df['call_duration'].cast('int') > 0
    ]
    validity_check = df.select(*validity_checks).filter(~col('validity')).count() == 0

    # Uniqueness: Ensure call_id is unique
    uniqueness_check = df.group_by('call_id').agg(count('call_id').alias('count')).filter(col('count') > 1).count() == 0

    # Integrity: Check foreign key references (assuming reference tables exist)
    # This is a placeholder for actual foreign key checks
    integrity_check = True

    # Consistency with Business Rules: Check call_duration calculation
    business_rule_check = (df['call_duration'] == (df['call_end_time'] - df['call_start_time']).cast('int')).filter(~col('business_rule')).count() == 0

    return {
        "completeness": completeness_check,
        "accuracy": accuracy_check,
        "consistency": consistency_check,
        "timeliness": timeliness_check,
        "validity": validity_check,
        "uniqueness": uniqueness_check,
        "integrity": integrity_check,
        "business_rule": business_rule_check
    }