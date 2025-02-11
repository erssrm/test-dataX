import pytest
from unittest.mock import MagicMock
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

# Mock data for testing
mock_data = [
    {"call_center_id": 1, "call_date": "2023-10-01", "agent_id": 101, "customer_id": 201, "call_duration": 300, "call_type": "inbound", "call_result": "resolved"},
    {"call_center_id": 2, "call_date": "2023-10-02", "agent_id": 102, "customer_id": 202, "call_duration": 0, "call_type": "outbound", "call_result": "unresolved"},
    {"call_center_id": 3, "call_date": "2023-10-03", "agent_id": 103, "customer_id": 203, "call_duration": 150, "call_type": "invalid", "call_result": "resolved"},
    {"call_center_id": 4, "call_date": "2023-10-04", "agent_id": 104, "customer_id": 204, "call_duration": 200, "call_type": "inbound", "call_result": "invalid"},
    {"call_center_id": 5, "call_date": "2023-10-05", "agent_id": None, "customer_id": 205, "call_duration": 250, "call_type": "inbound", "call_result": "resolved"},
    {"call_center_id": 6, "call_date": "2023-10-06", "agent_id": 106, "customer_id": 206, "call_duration": 100, "call_type": "outbound", "call_result": "resolved"},
    {"call_center_id": 6, "call_date": "2023-10-06", "agent_id": 106, "customer_id": 206, "call_duration": 100, "call_type": "outbound", "call_result": "resolved"}
]

@pytest.fixture
def mock_session():
    session = MagicMock(spec=Session)
    session.table.return_value = session.create_dataframe(mock_data)
    return session

def test_validate_call_center_data(mock_session):
    from rules import validate_call_center_data
    
    result = validate_call_center_data(mock_session, "call_center")
    
    # Completeness check
    assert result["completeness_check"].count() == 1  # One record with missing agent_id
    
    # Accuracy check
    assert result["accuracy_check"].count() == 2  # Two records with invalid call_type or call_result
    
    # Consistency check
    assert result["consistency_check"].count() == 1  # One record with call_duration <= 0
    
    # Timeliness check
    assert result["timeliness_check"].count() == 0  # No records with future call_date
    
    # Validity check
    assert result["validity_check"].count() == 0  # All records have valid data types
    
    # Uniqueness check
    assert result["uniqueness_check"].count() == 1  # One duplicate call_center_id

# Run the tests
if __name__ == "__main__":
    pytest.main()