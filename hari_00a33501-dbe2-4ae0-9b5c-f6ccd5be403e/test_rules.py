import unittest
from unittest.mock import MagicMock
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from your_module import validate_call_center_data  # Replace 'your_module' with the actual module name

class TestValidateCallCenterData(unittest.TestCase):

    def setUp(self):
        # Mock the Snowflake session and table
        self.session = MagicMock(spec=Session)
        self.df = self.session.table.return_value

    def test_completeness(self):
        # Test for completeness: all required fields are filled
        self.df.filter.return_value.count.return_value = 0
        result = validate_call_center_data(self.session, "call_center_data")
        self.assertTrue(result)

    def test_accuracy(self):
        # Test for accuracy: valid call_type and resolution_status
        self.df.filter.return_value.count.return_value = 0
        result = validate_call_center_data(self.session, "call_center_data")
        self.assertTrue(result)

    def test_consistency(self):
        # Test for consistency: call_duration matches the difference between call_end_time and call_start_time
        self.df.filter.return_value.count.return_value = 0
        result = validate_call_center_data(self.session, "call_center_data")
        self.assertTrue(result)

    def test_timeliness(self):
        # Test for timeliness: call_start_time is not greater than call_end_time
        self.df.filter.return_value.count.return_value = 0
        result = validate_call_center_data(self.session, "call_center_data")
        self.assertTrue(result)

    def test_validity(self):
        # Test for validity: call_duration is positive
        self.df.filter.return_value.count.return_value = 0
        result = validate_call_center_data(self.session, "call_center_data")
        self.assertTrue(result)

    def test_uniqueness(self):
        # Test for uniqueness: call_id is unique
        self.df.select.return_value.collect.return_value = [(100,)]
        self.df.count.return_value = 100
        result = validate_call_center_data(self.session, "call_center_data")
        self.assertTrue(result)

    def test_integrity(self):
        # Test for integrity: resolution_status is 'resolved' only if customer_feedback is not null
        self.df.filter.return_value.count.return_value = 0
        result = validate_call_center_data(self.session, "call_center_data")
        self.assertTrue(result)

if __name__ == '__main__':
    unittest.main()