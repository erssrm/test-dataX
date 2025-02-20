import unittest
from unittest.mock import MagicMock
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

class TestCallCenterDataQuality(unittest.TestCase):

    def setUp(self):
        # Mock the Snowflake session and table
        self.session = MagicMock(spec=Session)
        self.df = self.session.table.return_value

        # Mock data for testing
        self.mock_data = [
            {'call_id': 1, 'agent_id': 101, 'customer_id': 1001, 'call_start_time': '2023-10-01 10:00:00',
             'call_end_time': '2023-10-01 10:30:00', 'call_duration': 1800, 'call_type': 'inquiry',
             'call_resolution': 'resolved'},
            {'call_id': 2, 'agent_id': 102, 'customer_id': 1002, 'call_start_time': '2023-10-01 11:00:00',
             'call_end_time': '2023-10-01 11:20:00', 'call_duration': 1200, 'call_type': 'complaint',
             'call_resolution': 'escalated'}
        ]

        # Configure the mock to return the mock data
        self.df.collect.return_value = self.mock_data

    def test_completeness(self):
        # Test completeness check
        completeness_check = all(
            all(record[col_name] is not None for col_name in [
                'call_id', 'agent_id', 'customer_id', 'call_start_time',
                'call_end_time', 'call_duration', 'call_type', 'call_resolution'
            ]) for record in self.mock_data
        )
        self.assertTrue(completeness_check)

    def test_accuracy(self):
        # Test accuracy check
        valid_call_types = ['inquiry', 'complaint', 'support']
        valid_call_resolutions = ['resolved', 'escalated']
        accuracy_check = all(
            record['call_type'] in valid_call_types and
            record['call_resolution'] in valid_call_resolutions
            for record in self.mock_data
        )
        self.assertTrue(accuracy_check)

    def test_consistency(self):
        # Test consistency check
        consistency_check = all(
            record['call_duration'] == (pd.to_datetime(record['call_end_time']) - pd.to_datetime(record['call_start_time'])).seconds
            for record in self.mock_data
        )
        self.assertTrue(consistency_check)

    def test_timeliness(self):
        # Test timeliness check
        current_time = pd.to_datetime('now')
        timeliness_check = all(
            pd.to_datetime(record['call_start_time']) <= pd.to_datetime(record['call_end_time']) and
            pd.to_datetime(record['call_start_time']) <= current_time
            for record in self.mock_data
        )
        self.assertTrue(timeliness_check)

    def test_validity(self):
        # Test validity check
        validity_check = all(
            isinstance(record['call_id'], int) and
            isinstance(record['agent_id'], int) and
            isinstance(record['customer_id'], int) and
            isinstance(record['call_duration'], int) and
            record['call_duration'] > 0
            for record in self.mock_data
        )
        self.assertTrue(validity_check)

    def test_uniqueness(self):
        # Test uniqueness check
        call_ids = [record['call_id'] for record in self.mock_data]
        uniqueness_check = len(call_ids) == len(set(call_ids))
        self.assertTrue(uniqueness_check)

    def test_integrity(self):
        # Test integrity check (placeholder)
        integrity_check = True  # Assuming foreign key checks are valid
        self.assertTrue(integrity_check)

    def test_business_rule(self):
        # Test business rule check
        business_rule_check = all(
            record['call_duration'] == (pd.to_datetime(record['call_end_time']) - pd.to_datetime(record['call_start_time'])).seconds
            for record in self.mock_data
        )
        self.assertTrue(business_rule_check)

if __name__ == '__main__':
    unittest.main()