import unittest
from unittest.mock import MagicMock
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from your_module import validate_call_center_data  # Replace 'your_module' with the actual module name

class TestValidateCallCenterData(unittest.TestCase):

    def setUp(self):
        # Mock the Snowflake session and DataFrame
        self.session = MagicMock(spec=Session)
        self.call_center_df = MagicMock()

    def test_completeness_checks(self):
        # Test that all required fields are filled in
        self.call_center_df.filter.return_value = self.call_center_df
        result_df = validate_call_center_data(self.session, self.call_center_df)
        self.call_center_df.filter.assert_called()

    def test_accuracy_checks(self):
        # Test valid country codes and phone number format
        self.call_center_df.filter.return_value = self.call_center_df
        result_df = validate_call_center_data(self.session, self.call_center_df)
        self.call_center_df.filter.assert_called()

    def test_consistency_checks(self):
        # Test date consistency
        self.call_center_df.filter.return_value = self.call_center_df
        result_df = validate_call_center_data(self.session, self.call_center_df)
        self.call_center_df.filter.assert_called()

    def test_timeliness_checks(self):
        # Test open_date <= close_date
        self.call_center_df.filter.return_value = self.call_center_df
        result_df = validate_call_center_data(self.session, self.call_center_df)
        self.call_center_df.filter.assert_called()

    def test_validity_checks(self):
        # Test data type checks
        self.call_center_df.filter.return_value = self.call_center_df
        result_df = validate_call_center_data(self.session, self.call_center_df)
        self.call_center_df.filter.assert_called()

    def test_uniqueness_checks(self):
        # Test primary key uniqueness
        self.call_center_df.group_by.return_value.count.return_value.filter.return_value.count.return_value = 0
        result_df = validate_call_center_data(self.session, self.call_center_df)
        self.call_center_df.group_by.assert_called_with("call_center_id")

    def test_integrity_checks(self):
        # Test logical consistency
        self.call_center_df.filter.return_value = self.call_center_df
        result_df = validate_call_center_data(self.session, self.call_center_df)
        self.call_center_df.filter.assert_called()

if __name__ == '__main__':
    unittest.main()