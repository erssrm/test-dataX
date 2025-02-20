import unittest
from unittest.mock import MagicMock
from snowflake.snowpark import Session
from your_module import validate_call_details  # Replace 'your_module' with the actual module name

class TestValidateCallDetails(unittest.TestCase):

    def setUp(self):
        # Mock the Snowflake session and tables
        self.session = MagicMock(spec=Session)
        self.call_details = MagicMock()
        self.agent_details = MagicMock()
        self.customer_details = MagicMock()

        # Set up the mock tables
        self.session.table.side_effect = lambda table_name: {
            "Call_Details": self.call_details,
            "Agent_Details": self.agent_details,
            "Customer_Details": self.customer_details
        }[table_name]

    def test_completeness_check(self):
        # Mock the filter method to simulate completeness check
        self.call_details.filter.return_value = "completeness_check_result"
        result = validate_call_details(self.session)
        self.assertEqual(result["completeness_check"], "completeness_check_result")

    def test_accuracy_check(self):
        # Mock the filter method to simulate accuracy check
        self.call_details.filter.return_value = "accuracy_check_result"
        result = validate_call_details(self.session)
        self.assertEqual(result["accuracy_check"], "accuracy_check_result")

    def test_consistency_check(self):
        # Mock the join and select methods to simulate consistency check
        self.call_details.join.return_value.select.return_value.union.return_value = "consistency_check_result"
        result = validate_call_details(self.session)
        self.assertEqual(result["consistency_check"], "consistency_check_result")

    def test_timeliness_check(self):
        # Mock the filter method to simulate timeliness check
        self.call_details.filter.return_value = "timeliness_check_result"
        result = validate_call_details(self.session)
        self.assertEqual(result["timeliness_check"], "timeliness_check_result")

    def test_validity_check(self):
        # Mock the filter method to simulate validity check
        self.call_details.filter.return_value = "validity_check_result"
        result = validate_call_details(self.session)
        self.assertEqual(result["validity_check"], "validity_check_result")

    def test_uniqueness_check(self):
        # Mock the group_by and filter methods to simulate uniqueness check
        self.call_details.group_by.return_value.agg.return_value.filter.return_value = "uniqueness_check_result"
        result = validate_call_details(self.session)
        self.assertEqual(result["uniqueness_check"], "uniqueness_check_result")

    def test_integrity_check(self):
        # Mock the filter method to simulate integrity check
        self.call_details.filter.return_value = "integrity_check_result"
        result = validate_call_details(self.session)
        self.assertEqual(result["integrity_check"], "integrity_check_result")

if __name__ == '__main__':
    unittest.main()