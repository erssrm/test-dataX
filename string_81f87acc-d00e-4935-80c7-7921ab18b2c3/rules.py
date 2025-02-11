from pyspark.sql.functions import col, isnan, when, count, lit, length

def check_completeness(df):
    # Ensure all required fields are filled in
    required_fields = ['call_center_id', 'location']
    completeness_checks = [count(when(col(field).isNull() | isnan(col(field)), field)).alias(field) for field in required_fields]
    return df.select(completeness_checks)

def validate_location(df, valid_locations):
    # Validate location entries
    return df.filter(~col('location').isin(valid_locations))

def validate_numeric_ranges(df):
    # Ensure numerical fields are within realistic ranges
    return df.filter((col('number_of_calls') < 0) | (col('average_call_duration') < 0))

def check_consistency(df):
    # Ensure consistent use of data formats and units
    return df.filter(length(col('call_timestamp')) != 19)  # Assuming 'YYYY-MM-DD HH:MM:SS' format

def check_timeliness(df):
    # Verify that timestamps are logical
    return df.filter(col('call_timestamp') > lit('2023-10-01 00:00:00'))  # Example date

def check_uniqueness(df):
    # Ensure call_center_id is unique
    return df.groupBy('call_center_id').count().filter(col('count') > 1)

def check_integrity(df, related_df):
    # Ensure foreign keys reference valid records
    return df.join(related_df, df['related_id'] == related_df['id'], 'left_anti')

def validate_business_rules(df):
    # Validate business rules
    return df.filter(col('customer_satisfaction') < 3)  # Example rule

# Example usage
# df = spark.read.csv('call_center.csv', header=True, inferSchema=True)
# valid_locations = ['New York', 'Los Angeles', 'Chicago']
# related_df = spark.read.csv('related_table.csv', header=True, inferSchema=True)

# completeness_issues = check_completeness(df)
# location_issues = validate_location(df, valid_locations)
# numeric_issues = validate_numeric_ranges(df)
# consistency_issues = check_consistency(df)
# timeliness_issues = check_timeliness(df)
# uniqueness_issues = check_uniqueness(df)
# integrity_issues = check_integrity(df, related_df)
# business_rule_issues = validate_business_rules(df)