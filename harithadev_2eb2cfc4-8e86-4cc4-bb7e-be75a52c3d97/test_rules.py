import unittest
from unittest.mock import MagicMock
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

# Assuming the functions are imported from the module
from your_module import (
    check_completeness,
    check_accuracy,
    check_uniqueness,
    check_consistency,
    check_timeliness,
    apply_quality_checks
)

class TestQualityChecks(unittest.TestCase):

    def setUp(self):
        # Mock the Snowflake session
        self.session = MagicMock(spec=Session)

    def test_check_completeness(self):
        # Mock the DataFrame
        df_mock = MagicMock()
        self.session.table.return_value = df_mock

        # Call the function
        result_df = check_completeness(self.session, "Call_Details", ["call_id", "timestamp"])

        # Check if the with_column method was called for each required column
        self.assertTrue(df_mock.with_column.called)
        self.assertEqual(df_mock.with_column.call_count, 2)

    def test_check_accuracy(self):
        # Mock the DataFrame
        df_mock = MagicMock()
        self.session.table.return_value = df_mock

        # Call the function
        result_df = check_accuracy(self.session, "Call_Details", "call_type", ["inbound", "outbound"])

        # Check if the with_column method was called
        self.assertTrue(df_mock.with_column.called)

    def test_check_uniqueness(self):
        # Mock the DataFrame
        df_mock = MagicMock()
        self.session.table.return_value = df_mock

        # Call the function
        result_df = check_uniqueness(self.session, "Call_Details", "call_id")

        # Check if the with_column method was called
        self.assertTrue(df_mock.with_column.called)

    def test_check_consistency(self):
        # Mock the DataFrame
        df_mock = MagicMock()
        ref_df_mock = MagicMock()
        self.session.table.side_effect = [df_mock, ref_df_mock]

        # Call the function
        result_df = check_consistency(self.session, "Call_Details", "agent_id", "Agent_Details", "agent_id")

        # Check if the join and with_column methods were called
        self.assertTrue(df_mock.join.called)
        self.assertTrue(df_mock.with_column.called)

    def test_check_timeliness(self):
        # Mock the DataFrame
        df_mock = MagicMock()
        self.session.table.return_value = df_mock

        # Call the function
        result_df = check_timeliness(self.session, "Call_Details", "timestamp", "2023-12-31")

        # Check if the with_column method was called
        self.assertTrue(df_mock.with_column.called)

if __name__ == '__main__':
    unittest.main()