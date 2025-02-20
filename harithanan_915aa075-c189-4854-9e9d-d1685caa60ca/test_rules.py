import unittest
from unittest.mock import MagicMock
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

# Assuming the validation functions are imported from the module
from your_module import (
    validate_call_details,
    validate_agent_details,
    validate_customer_details,
    validate_call_queue,
    validate_call_resolution
)

class TestDataQualityRules(unittest.TestCase):

    def setUp(self):
        # Create a mock session
        self.session = MagicMock(spec=Session)

    def test_validate_call_details(self):
        # Mock the Call_Details table
        call_details_mock = self.session.table.return_value
        call_details_mock.filter.return_value = call_details_mock
        call_details_mock.group_by.return_value.count.return_value.filter.return_value = call_details_mock

        # Call the validation function
        completeness_check, accuracy_check, consistency_check, uniqueness_check = validate_call_details(self.session)

        # Assert that the filter method was called for completeness, accuracy, and consistency checks
        self.assertTrue(call_details_mock.filter.called)
        # Assert that the group_by and count methods were called for uniqueness check
        self.assertTrue(call_details_mock.group_by.called)
        self.assertTrue(call_details_mock.group_by.return_value.count.called)

    def test_validate_agent_details(self):
        # Mock the Agent_Details table
        agent_details_mock = self.session.table.return_value
        agent_details_mock.filter.return_value = agent_details_mock
        agent_details_mock.group_by.return_value.count.return_value.filter.return_value = agent_details_mock

        # Call the validation function
        completeness_check, accuracy_check, uniqueness_check = validate_agent_details(self.session)

        # Assert that the filter method was called for completeness and accuracy checks
        self.assertTrue(agent_details_mock.filter.called)
        # Assert that the group_by and count methods were called for uniqueness check
        self.assertTrue(agent_details_mock.group_by.called)
        self.assertTrue(agent_details_mock.group_by.return_value.count.called)

    def test_validate_customer_details(self):
        # Mock the Customer_Details table
        customer_details_mock = self.session.table.return_value
        customer_details_mock.filter.return_value = customer_details_mock
        customer_details_mock.group_by.return_value.count.return_value.filter.return_value = customer_details_mock

        # Call the validation function
        completeness_check, uniqueness_check = validate_customer_details(self.session)

        # Assert that the filter method was called for completeness check
        self.assertTrue(customer_details_mock.filter.called)
        # Assert that the group_by and count methods were called for uniqueness check
        self.assertTrue(customer_details_mock.group_by.called)
        self.assertTrue(customer_details_mock.group_by.return_value.count.called)

    def test_validate_call_queue(self):
        # Mock the Call_Queue table
        call_queue_mock = self.session.table.return_value
        call_queue_mock.filter.return_value = call_queue_mock

        # Call the validation function
        completeness_check, referential_integrity_check = validate_call_queue(self.session)

        # Assert that the filter method was called for completeness and referential integrity checks
        self.assertTrue(call_queue_mock.filter.called)

    def test_validate_call_resolution(self):
        # Mock the Call_Resolution table
        call_resolution_mock = self.session.table.return_value
        call_resolution_mock.filter.return_value = call_resolution_mock

        # Call the validation function
        completeness_check, referential_integrity_check = validate_call_resolution(self.session)

        # Assert that the filter method was called for completeness and referential integrity checks
        self.assertTrue(call_resolution_mock.filter.called)

if __name__ == '__main__':
    unittest.main()