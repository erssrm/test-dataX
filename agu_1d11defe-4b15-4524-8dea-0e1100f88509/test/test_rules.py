import pytest
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from your_module import validate_call_center_data  # Replace with the actual module name

@pytest.fixture
def session():
    # Mock or create a Snowflake session for testing
    return Session.builder.configs({...}).create()  # Add necessary configurations

@pytest.fixture
def call_center_df(session):
    # Create a mock DataFrame for testing
    data = [
        {"CALL_CENTER_ID": 1, "CALL_CENTER_NAME": "Center A", "LOCATION": "Location1", "OPENING_DATE": "2023-01-01", "MANAGER_ID": 101, "TOTAL_CALLS": 100, "SUCCESSFUL_CALLS": 80, "FAILED_CALLS": 20, "AVERAGE_WAIT_TIME": 5.0},
        {"CALL_CENTER_ID": 2, "CALL_CENTER_NAME": "Center B", "LOCATION": "InvalidLocation", "OPENING_DATE": "2023-01-01", "MANAGER_ID": 102, "TOTAL_CALLS": 50, "SUCCESSFUL_CALLS": 30, "FAILED_CALLS": 25, "AVERAGE_WAIT_TIME": 3.5},
        # Add more test data as needed
    ]
    return session.create_dataframe(data)

def test_completeness(session, call_center_df):
    result = validate_call_center_data(session, call_center_df)
    assert result.filter(col("CALL_CENTER_ID").is_null()).count() == 0
    assert result.filter(col("CALL_CENTER_NAME").is_null()).count() == 0
    assert result.filter(col("LOCATION").is_null()).count() == 0
    assert result.filter(col("OPENING_DATE").is_null()).count() == 0
    assert result.filter(col("MANAGER_ID").is_null()).count() == 0

def test_accuracy(session, call_center_df):
    result = validate_call_center_data(session, call_center_df)
    assert result.filter(~col("LOCATION").isin(["Location1", "Location2", "Location3"])).count() > 0
    assert result.filter(~col("OPENING_DATE").rlike(r"^\d{4}-\d{2}-\d{2}$")).count() == 0

def test_consistency(session, call_center_df):
    # Assuming manager_df is available in the session
    manager_df = session.table("manager")
    result = validate_call_center_data(session, call_center_df)
    assert result.join(manager_df, call_center_df["MANAGER_ID"] == manager_df["MANAGER_ID"], "left_anti").count() == 0

def test_timeliness(session, call_center_df):
    result = validate_call_center_data(session, call_center_df)
    assert result.filter(col("OPENING_DATE") > col("current_date()")).count() == 0

def test_validity(session, call_center_df):
    result = validate_call_center_data(session, call_center_df)
    assert result.filter(col("TOTAL_CALLS") < 0).count() == 0
    assert result.filter(col("SUCCESSFUL_CALLS") < 0).count() == 0
    assert result.filter(col("FAILED_CALLS") < 0).count() == 0
    assert result.filter(col("AVERAGE_WAIT_TIME") < 0).count() == 0

def test_uniqueness(session, call_center_df):
    result = validate_call_center_data(session, call_center_df)
    assert result.group_by("CALL_CENTER_ID").count().filter(col("count") > 1).count() == 0

def test_integrity(session, call_center_df):
    result = validate_call_center_data(session, call_center_df)
    assert result.filter((col("SUCCESSFUL_CALLS") + col("FAILED_CALLS")) > col("TOTAL_CALLS")).count() == 0

# Add more tests as needed for compliance and other checks