import unittest
from unittest.mock import MagicMock
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

# Mock data for testing
mock_call_details_data = [
    {"call_id": 1, "timestamp": "2023-10-01 10:00:00", "duration": 300, "agent_id": 101, "customer_id": 201, "call_type": "inbound", "call_outcome": "completed"},
    {"call_id": 2, "timestamp": "2023-10-01 11:00:00", "duration": 200, "agent_id": 102, "customer_id": 202, "call_type": "outbound", "call_outcome": "missed"},
    # Add more mock data as needed
]

mock_agent_details_data = [
    {"agent_id": 101, "name": "Agent A", "department": "Sales", "shift_details": "Morning"},
    {"agent_id": 102, "name": "Agent B", "department": "Support", "shift_details": "Evening"},
    # Add more mock data as needed
]

mock_customer_details_data = [
    {"customer_id": 201, "name": "Customer X", "contact_info": "1234567890"},
    {"customer_id": 202, "name": "Customer Y", "contact_info": "0987654321"},
    # Add more mock data as needed
]

mock_call_queue_data = [
    {"queue_id": 1, "call_id": 1, "position": 1, "wait_time": 30},
    {"queue_id": 2, "call_id": 2, "position": 2, "wait_time": 45},
    # Add more mock data as needed
]

mock_call_resolution_data = [
    {"resolution_id": 1, "call_id": 1, "resolution_type": "Resolved", "resolution_time": "2023-10-01 10:30:00"},
    {"resolution_id": 2, "call_id": 2, "resolution_type": "Unresolved", "resolution_time": "2023-10-01 11:30:00"},
    # Add more mock data as needed
]

class TestDataQualityChecks(unittest.TestCase):

    def setUp(self):
        # Mock the Snowpark session and tables
        self.session = MagicMock(spec=Session)
        self.session.table = MagicMock(side_effect=self.mock_table)

    def mock_table(self, table_name):
        # Return mock data based on table name
        if table_name == "Call_Details":
            return mock_call_details_data
        elif table_name == "Agent_Details":
            return mock_agent_details_data
        elif table_name == "Customer_Details":
            return mock_customer_details_data
        elif table_name == "Call_Queue":
            return mock_call_queue_data
        elif table_name == "Call_Resolution":
            return mock_call_resolution_data
        else:
            return []

    def test_validate_call_details(self):
        completeness_check, accuracy_check, uniqueness_check = validate_call_details(self.session)
        self.assertEqual(len(completeness_check), 0)
        self.assertEqual(len(accuracy_check), 0)
        self.assertEqual(len(uniqueness_check), 0)

    def test_validate_agent_details(self):
        completeness_check, uniqueness_check = validate_agent_details(self.session)
        self.assertEqual(len(completeness_check), 0)
        self.assertEqual(len(uniqueness_check), 0)

    def test_validate_customer_details(self):
        completeness_check, uniqueness_check = validate_customer_details(self.session)
        self.assertEqual(len(completeness_check), 0)
        self.assertEqual(len(uniqueness_check), 0)

    def test_validate_call_queue(self):
        completeness_check, referential_integrity_check = validate_call_queue(self.session)
        self.assertEqual(len(completeness_check), 0)
        self.assertEqual(len(referential_integrity_check), 0)

    def test_validate_call_resolution(self):
        completeness_check, referential_integrity_check = validate_call_resolution(self.session)
        self.assertEqual(len(completeness_check), 0)
        self.assertEqual(len(referential_integrity_check), 0)

if __name__ == '__main__':
    unittest.main()