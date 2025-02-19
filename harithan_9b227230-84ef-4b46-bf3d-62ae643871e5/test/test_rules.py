import unittest
from unittest.mock import MagicMock
from snowflake.snowpark import Session
from your_module import validate_call_center_data  # Replace 'your_module' with the actual module name

class TestValidateCallCenterData(unittest.TestCase):

    def setUp(self):
        # Mock the Snowflake session and table
        self.session = MagicMock(spec=Session)
        self.mock_table = MagicMock()
        self.session.table.return_value = self.mock_table

    def test_completeness_checks(self):
        # Simulate a DataFrame with missing values
        self.mock_table.filter.return_value.count.side_effect = [1, 0, 0, 0, 0, 0, 0]
        errors = validate_call_center_data(self.session, "call_center_data")
        self.assertIn(True, errors, "Completeness check failed to detect missing call_id")

    def test_accuracy_checks(self):
        # Simulate a DataFrame with invalid call_type
        self.mock_table.filter.return_value.count.side_effect = [0, 1]
        errors = validate_call_center_data(self.session, "call_center_data")
        self.assertIn(True, errors, "Accuracy check failed to detect invalid call_type")

    def test_consistency_checks(self):
        # Simulate a DataFrame with inconsistent call_duration
        self.mock_table.filter.return_value.count.return_value = 1
        errors = validate_call_center_data(self.session, "call_center_data")
        self.assertIn(True, errors, "Consistency check failed to detect inconsistent call_duration")

    def test_timeliness_checks(self):
        # Simulate a DataFrame with future call_start_time
        self.mock_table.filter.return_value.count.side_effect = [1, 0]
        errors = validate_call_center_data(self.session, "call_center_data")
        self.assertIn(True, errors, "Timeliness check failed to detect future call_start_time")

    def test_validity_checks(self):
        # Simulate a DataFrame with non-positive call_duration
        self.mock_table.filter.return_value.count.return_value = 1
        errors = validate_call_center_data(self.session, "call_center_data")
        self.assertIn(True, errors, "Validity check failed to detect non-positive call_duration")

    def test_uniqueness_checks(self):
        # Simulate a DataFrame with duplicate call_id
        self.mock_table.select.return_value.collect.return_value = [(1,)]
        self.mock_table.count.return_value = 2
        errors = validate_call_center_data(self.session, "call_center_data")
        self.assertIn(True, errors, "Uniqueness check failed to detect duplicate call_id")

    def test_integrity_checks(self):
        # Simulate a DataFrame with invalid agent_id
        self.mock_table.join.return_value.count.return_value = 1
        errors = validate_call_center_data(self.session, "call_center_data")
        self.assertIn(True, errors, "Integrity check failed to detect invalid agent_id")

    def test_business_rule_checks(self):
        # Simulate a DataFrame with call_end_time preceding call_start_time
        self.mock_table.filter.return_value.count.return_value = 1
        errors = validate_call_center_data(self.session, "call_center_data")
        self.assertIn(True, errors, "Business rule check failed to detect call_end_time preceding call_start_time")

if __name__ == '__main__':
    unittest.main()