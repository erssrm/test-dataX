import unittest
from unittest.mock import MagicMock
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

# Assuming the functions are imported from the module
from your_module import (
    check_completeness,
    check_accuracy,
    check_consistency,
    check_uniqueness,
    check_referential_integrity,
    check_timeliness,
    apply_quality_checks
)

class TestDataQualityChecks(unittest.TestCase):

    def setUp(self):
        # Mock the Snowflake session
        self.session = MagicMock(spec=Session)

    def test_check_completeness(self):
        # Mock the DataFrame
        df_mock = MagicMock()
        self.session.table.return_value = df_mock

        # Call the function
        result_df = check_completeness(self.session, "Call_Details", ["call_id", "timestamp"])

        # Check if the function adds the correct columns
        df_mock.with_column.assert_any_call("call_id_is_null", col("call_id").is_null())
        df_mock.with_column.assert_any_call("timestamp_is_null", col("timestamp").is_null())

    def test_check_accuracy(self):
        # Mock the DataFrame
        df_mock = MagicMock()
        self.session.table.return_value = df_mock

        # Call the function
        result_df = check_accuracy(self.session, "Call_Details", "call_type", ["inbound", "outbound"])

        # Check if the function adds the correct column
        df_mock.with_column.assert_called_with("call_type_is_valid", col("call_type").isin(["inbound", "outbound"]))

    def test_check_consistency(self):
        # Mock the DataFrame
        df_mock = MagicMock()
        self.session.table.return_value = df_mock

        # Call the function
        result_df = check_consistency(self.session, "Call_Details", "timestamp", r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")

        # Check if the function adds the correct column
        df_mock.with_column.assert_called_with("timestamp_is_consistent", col("timestamp").rlike(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"))

    def test_check_uniqueness(self):
        # Mock the DataFrame
        df_mock = MagicMock()
        self.session.table.return_value = df_mock

        # Call the function
        result_df = check_uniqueness(self.session, "Call_Details", "call_id")

        # Check if the function performs the correct operations
        df_mock.group_by.assert_called_with("call_id")
        df_mock.group_by().count().filter.assert_called_with(col("count") > 1)

    def test_check_referential_integrity(self):
        # Mock the DataFrame
        df_mock = MagicMock()
        ref_df_mock = MagicMock()
        self.session.table.side_effect = [df_mock, ref_df_mock]

        # Call the function
        result_df = check_referential_integrity(self.session, "Call_Details", "agent_id", "Agent_Details", "agent_id")

        # Check if the function performs the correct join
        df_mock.join.assert_called_with(ref_df_mock, df_mock["agent_id"] == ref_df_mock["agent_id"], "left_anti")

    def test_check_timeliness(self):
        # Mock the DataFrame
        df_mock = MagicMock()
        self.session.table.return_value = df_mock

        # Call the function
        result_df = check_timeliness(self.session, "Call_Details", "timestamp", "2023-01-01", "2023-12-31")

        # Check if the function adds the correct column
        df_mock.with_column.assert_called_with("timestamp_is_timely", (col("timestamp") >= "2023-01-01") & (col("timestamp") <= "2023-12-31"))

if __name__ == '__main__':
    unittest.main()