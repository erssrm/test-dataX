import unittest
from unittest.mock import MagicMock
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from your_module import validate_call_center_data  # Replace with the actual module name

class TestCallCenterDataQuality(unittest.TestCase):

    def setUp(self):
        # Mock the Snowpark session and table
        self.session = MagicMock(spec=Session)
        self.df_mock = MagicMock()
        self.session.table.return_value = self.df_mock

    def test_completeness_check(self):
        # Mock the filter method to simulate completeness issues
        self.df_mock.filter.return_value = self.df_mock
        self.df_mock.filter.return_value.count.return_value = 1

        result = validate_call_center_data(self.session, "call_center_table")
        self.assertGreater(result["completeness_issues"].count(), 0)

    def test_accuracy_check(self):
        # Mock the filter method to simulate accuracy issues
        self.df_mock.filter.return_value = self.df_mock
        self.df_mock.filter.return_value.count.return_value = 1

        result = validate_call_center_data(self.session, "call_center_table")
        self.assertGreater(result["accuracy_issues"].count(), 0)

    def test_consistency_check(self):
        # Mock the filter method to simulate consistency issues
        self.df_mock.filter.return_value = self.df_mock
        self.df_mock.filter.return_value.count.return_value = 1

        result = validate_call_center_data(self.session, "call_center_table")
        self.assertGreater(result["consistency_issues"].count(), 0)

    def test_timeliness_check(self):
        # Mock the filter method to simulate timeliness issues
        self.df_mock.filter.return_value = self.df_mock
        self.df_mock.filter.return_value.count.return_value = 1

        result = validate_call_center_data(self.session, "call_center_table")
        self.assertGreater(result["timeliness_issues"].count(), 0)

    def test_validity_check(self):
        # Mock the filter method to simulate validity issues
        self.df_mock.filter.return_value = self.df_mock
        self.df_mock.filter.return_value.count.return_value = 1

        result = validate_call_center_data(self.session, "call_center_table")
        self.assertGreater(result["validity_issues"].count(), 0)

    def test_uniqueness_check(self):
        # Mock the filter method to simulate uniqueness issues
        self.df_mock.group_by.return_value.count.return_value = self.df_mock
        self.df_mock.group_by.return_value.count.return_value.filter.return_value.count.return_value = 1

        result = validate_call_center_data(self.session, "call_center_table")
        self.assertGreater(result["uniqueness_issues"].count(), 0)

    def test_integrity_check(self):
        # Mock the filter method to simulate integrity issues
        self.df_mock.filter.return_value = self.df_mock
        self.df_mock.filter.return_value.count.return_value = 1

        result = validate_call_center_data(self.session, "call_center_table")
        self.assertGreater(result["integrity_issues"].count(), 0)

    def test_business_rule_check(self):
        # Mock the filter method to simulate business rule issues
        self.df_mock.filter.return_value = self.df_mock
        self.df_mock.filter.return_value.count.return_value = 1

        result = validate_call_center_data(self.session, "call_center_table")
        self.assertGreater(result["business_rule_issues"].count(), 0)

if __name__ == '__main__':
    unittest.main()