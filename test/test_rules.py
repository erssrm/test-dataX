import pytest
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from your_module import validate_call_center_data  # Replace with the actual module name

@pytest.fixture
def session():
    # Create a Snowflake session for testing
    return Session.builder.configs(...).create()

@pytest.fixture
def call_center_df(session):
    # Create a mock DataFrame for testing
    data = [
        {"call_id": 1, "agent_id": 101, "call_start_time": "2023-10-01 10:00:00", "call_end_time": "2023-10-01 10:30:00", "call_type": "inbound", "phone_number": "+1234567890", "call_duration": 1800, "external_number": None},
        {"call_id": 2, "agent_id": 102, "call_start_time": "2023-10-01 11:00:00", "call_end_time": "2023-10-01 11:30:00", "call_type": "outbound", "phone_number": "+0987654321", "call_duration": 1800, "external_number": "+0987654321"},
        # Add more test data as needed
    ]
    return session.create_dataframe(data)

def test_completeness_check(session, call_center_df):
    # Test for completeness check
    incomplete_data = call_center_df.filter(col("call_id").is_null())
    assert incomplete_data.count() == 0

def test_valid_values_check(session, call_center_df):
    # Test for valid values check
    invalid_values = call_center_df.filter(~col("call_type").isin(["inbound", "outbound", "internal"]))
    assert invalid_values.count() == 0

def test_format_compliance_check(session, call_center_df):
    # Test for format compliance check
    invalid_format = call_center_df.filter(~col("phone_number").rlike(r"^\+?[1-9]\d{1,14}$"))
    assert invalid_format.count() == 0

def test_referential_integrity_check(session, call_center_df):
    # Test for referential integrity check
    # Assuming 'agent' table is available in the session
    referential_issues = call_center_df.join(session.table("agent"), call_center_df["agent_id"] == col("agent.agent_id"), "left_anti")
    assert referential_issues.count() == 0

def test_timestamp_validation_check(session, call_center_df):
    # Test for timestamp validation check
    invalid_timestamps = call_center_df.filter(col("call_start_time") > col("call_end_time"))
    assert invalid_timestamps.count() == 0

def test_range_check(session, call_center_df):
    # Test for range check
    out_of_range = call_center_df.filter((col("call_duration") < 0) | (col("call_duration") > 86400))
    assert out_of_range.count() == 0

def test_uniqueness_check(session, call_center_df):
    # Test for uniqueness check
    duplicates = call_center_df.group_by("call_id").count().filter(col("count") > 1)
    assert duplicates.count() == 0

def test_logical_consistency_check(session, call_center_df):
    # Test for logical consistency check
    logical_issues = call_center_df.filter(col("call_start_time") > col("call_end_time"))
    assert logical_issues.count() == 0

def test_business_logic_check(session, call_center_df):
    # Test for business logic validation
    business_issues = call_center_df.filter((col("call_type") == "internal") & (col("external_number").is_not_null()))
    assert business_issues.count() == 0

# Run the tests
if __name__ == "__main__":
    pytest.main()