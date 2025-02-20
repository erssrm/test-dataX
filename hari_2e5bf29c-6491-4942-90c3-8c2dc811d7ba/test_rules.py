import unittest
from unittest.mock import MagicMock
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

# Assuming the functions are imported from the module
from your_module import (
    check_completeness_call_details,
    check_accuracy_call_details,
    check_uniqueness_call_details,
    check_completeness_agent_details,
    check_uniqueness_agent_details,
    check_completeness_customer_details,
    check_uniqueness_customer_details,
    check_completeness_call_queue,
    check_completeness_call_resolution,
    check_accuracy_call_resolution
)

class TestDataQualityChecks(unittest.TestCase):

    def setUp(self):
        # Mock the Snowpark session and table
        self.session = MagicMock(spec=Session)
        self.mock_df = MagicMock()
        self.session.table.return_value = self.mock_df

    def test_check_completeness_call_details(self):
        # Setup mock data
        self.mock_df.select.return_value.filter.return_value = self.mock_df
        result = check_completeness_call_details(self.session)
        self.mock_df.select.assert_called()
        self.mock_df.filter.assert_called()
        self.assertEqual(result, self.mock_df)

    def test_check_accuracy_call_details(self):
        # Setup mock data
        self.mock_df.select.return_value.filter.return_value = self.mock_df
        result = check_accuracy_call_details(self.session)
        self.mock_df.filter.assert_called()
        self.assertEqual(result, self.mock_df)

    def test_check_uniqueness_call_details(self):
        # Setup mock data
        self.mock_df.group_by.return_value.count.return_value.filter.return_value = self.mock_df
        result = check_uniqueness_call_details(self.session)
        self.mock_df.group_by.assert_called_with("call_id")
        self.mock_df.filter.assert_called()
        self.assertEqual(result, self.mock_df)

    def test_check_completeness_agent_details(self):
        # Setup mock data
        self.mock_df.select.return_value.filter.return_value = self.mock_df
        result = check_completeness_agent_details(self.session)
        self.mock_df.select.assert_called()
        self.mock_df.filter.assert_called()
        self.assertEqual(result, self.mock_df)

    def test_check_uniqueness_agent_details(self):
        # Setup mock data
        self.mock_df.group_by.return_value.count.return_value.filter.return_value = self.mock_df
        result = check_uniqueness_agent_details(self.session)
        self.mock_df.group_by.assert_called_with("agent_id")
        self.mock_df.filter.assert_called()
        self.assertEqual(result, self.mock_df)

    def test_check_completeness_customer_details(self):
        # Setup mock data
        self.mock_df.select.return_value.filter.return_value = self.mock_df
        result = check_completeness_customer_details(self.session)
        self.mock_df.select.assert_called()
        self.mock_df.filter.assert_called()
        self.assertEqual(result, self.mock_df)

    def test_check_uniqueness_customer_details(self):
        # Setup mock data
        self.mock_df.group_by.return_value.count.return_value.filter.return_value = self.mock_df
        result = check_uniqueness_customer_details(self.session)
        self.mock_df.group_by.assert_called_with("customer_id")
        self.mock_df.filter.assert_called()
        self.assertEqual(result, self.mock_df)

    def test_check_completeness_call_queue(self):
        # Setup mock data
        self.mock_df.select.return_value.filter.return_value = self.mock_df
        result = check_completeness_call_queue(self.session)
        self.mock_df.select.assert_called()
        self.mock_df.filter.assert_called()
        self.assertEqual(result, self.mock_df)

    def test_check_completeness_call_resolution(self):
        # Setup mock data
        self.mock_df.select.return_value.filter.return_value = self.mock_df
        result = check_completeness_call_resolution(self.session)
        self.mock_df.select.assert_called()
        self.mock_df.filter.assert_called()
        self.assertEqual(result, self.mock_df)

    def test_check_accuracy_call_resolution(self):
        # Setup mock data
        self.mock_df.select.return_value.filter.return_value = self.mock_df
        result = check_accuracy_call_resolution(self.session)
        self.mock_df.filter.assert_called()
        self.assertEqual(result, self.mock_df)

if __name__ == '__main__':
    unittest.main()