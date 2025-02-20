import unittest
from unittest.mock import MagicMock
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

# Import the functions to be tested
from rules import (
    validate_call_details,
    validate_agent_details,
    validate_customer_details,
    validate_call_queue,
    validate_call_resolution
)

class TestDataQualityChecks(unittest.TestCase):

    def setUp(self):
        # Mock the Snowpark session and table
        self.session = MagicMock(spec=Session)
        self.mock_table = MagicMock()
        self.session.table.return_value = self.mock_table

    def test_validate_call_details(self):
        # Mock data for Call_Details
        self.mock_table.filter.return_value = self.mock_table
        self.mock_table.group_by.return_value.count.return_value.filter.return_value = self.mock_table

        # Call the function
        result = validate_call_details(self.session)

        # Check if the function returns a dictionary with expected keys
        self.assertIsInstance(result, dict)
        self.assertIn("completeness", result)
        self.assertIn("accuracy", result)
        self.assertIn("consistency", result)
        self.assertIn("uniqueness", result)
        self.assertIn("timeliness", result)

    def test_validate_agent_details(self):
        # Mock data for Agent_Details
        self.mock_table.filter.return_value = self.mock_table
        self.mock_table.group_by.return_value.count.return_value.filter.return_value = self.mock_table

        # Call the function
        result = validate_agent_details(self.session)

        # Check if the function returns a dictionary with expected keys
        self.assertIsInstance(result, dict)
        self.assertIn("completeness", result)
        self.assertIn("accuracy", result)
        self.assertIn("uniqueness", result)
        self.assertIn("timeliness", result)

    def test_validate_customer_details(self):
        # Mock data for Customer_Details
        self.mock_table.filter.return_value = self.mock_table
        self.mock_table.group_by.return_value.count.return_value.filter.return_value = self.mock_table

        # Call the function
        result = validate_customer_details(self.session)

        # Check if the function returns a dictionary with expected keys
        self.assertIsInstance(result, dict)
        self.assertIn("completeness", result)
        self.assertIn("accuracy", result)
        self.assertIn("uniqueness", result)

    def test_validate_call_queue(self):
        # Mock data for Call_Queue
        self.mock_table.filter.return_value = self.mock_table
        self.mock_table.group_by.return_value.count.return_value.filter.return_value = self.mock_table

        # Call the function
        result = validate_call_queue(self.session)

        # Check if the function returns a dictionary with expected keys
        self.assertIsInstance(result, dict)
        self.assertIn("completeness", result)
        self.assertIn("consistency", result)
        self.assertIn("uniqueness", result)

    def test_validate_call_resolution(self):
        # Mock data for Call_Resolution
        self.mock_table.filter.return_value = self.mock_table
        self.mock_table.group_by.return_value.count.return_value.filter.return_value = self.mock_table

        # Call the function
        result = validate_call_resolution(self.session)

        # Check if the function returns a dictionary with expected keys
        self.assertIsInstance(result, dict)
        self.assertIn("completeness", result)
        self.assertIn("accuracy", result)
        self.assertIn("uniqueness", result)

if __name__ == '__main__':
    unittest.main()