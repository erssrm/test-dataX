import pytest
from unittest.mock import MagicMock
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from your_module import validate_call_center_data  # Replace with the actual module name

@pytest.fixture
def mock_session():
    # Create a mock Snowflake session
    return MagicMock(spec=Session)

@pytest.fixture
def mock_call_center_df(mock_session):
    # Create a mock DataFrame with sample data
    data = [
        {"cc_call_center_sk": 1, "cc_call_center_id": "CC1", "cc_name": "Center1", "cc_open_date_sk": 20200101, "cc_country": "ValidCountry1", "cc_state": "ValidState1", "cc_zip": "12345", "cc_rec_start_date": 20200101, "cc_rec_end_date": 20201231, "cc_closed_date_sk": 20201231, "cc_tax_percentage": 10, "cc_mkt_id": 1},
        {"cc_call_center_sk": 2, "cc_call_center_id": "CC2", "cc_name": "Center2", "cc_open_date_sk": 20200101, "cc_country": "InvalidCountry", "cc_state": "InvalidState", "cc_zip": "1234", "cc_rec_start_date": 20200101, "cc_rec_end_date": 20191231, "cc_closed_date_sk": 20191231, "cc_tax_percentage": 110, "cc_mkt_id": None},
    ]
    df = mock_session.create_dataframe(data)
    return df

def test_validate_call_center_data(mock_session, mock_call_center_df):
    # Call the validation function
    valid_df = validate_call_center_data(mock_session, mock_call_center_df)

    # Collect the results
    results = valid_df.collect()

    # Assert that only the valid record is returned
    assert len(results) == 1
    assert results[0]["cc_call_center_id"] == "CC1"

if __name__ == "__main__":
    pytest.main()