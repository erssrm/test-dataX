import unittest
from unittest.mock import MagicMock
from snowflake.snowpark import Session
from rules import (
    validate_calls_table,
    validate_agents_table,
    validate_customers_table,
    validate_interactions_table,
    validate_outcomes_table
)

class TestDataQualityRules(unittest.TestCase):

    def setUp(self):
        # Mock the Snowflake session
        self.session = MagicMock(spec=Session)

    def test_validate_calls_table(self):
        # Mock the calls table data
        calls_data = [
            {"call_id": 1, "timestamp": "2023-10-01 10:00:00", "duration": 300, "agent_id": 101, "customer_id": 201, "call_type": "inbound", "outcome": "resolved"},
            {"call_id": 2, "timestamp": "2023-10-01 11:00:00", "duration": 200, "agent_id": 102, "customer_id": 202, "call_type": "outbound", "outcome": "unresolved"}
        ]
        self.session.table.return_value.collect.return_value = calls_data

        # Test the validation function
        result = validate_calls_table(self.session)
        self.assertTrue(result)

    def test_validate_agents_table(self):
        # Mock the agents table data
        agents_data = [
            {"agent_id": 101, "name": "Agent A", "shift": "morning"},
            {"agent_id": 102, "name": "Agent B", "shift": "evening"}
        ]
        self.session.table.return_value.collect.return_value = agents_data

        # Test the validation function
        result = validate_agents_table(self.session)
        self.assertTrue(result)

    def test_validate_customers_table(self):
        # Mock the customers table data
        customers_data = [
            {"customer_id": 201, "name": "Customer A", "contact_details": "1234567890"},
            {"customer_id": 202, "name": "Customer B", "contact_details": "0987654321"}
        ]
        self.session.table.return_value.collect.return_value = customers_data

        # Test the validation function
        result = validate_customers_table(self.session)
        self.assertTrue(result)

    def test_validate_interactions_table(self):
        # Mock the interactions table data
        interactions_data = [
            {"interaction_id": 301, "agent_id": 101, "customer_id": 201},
            {"interaction_id": 302, "agent_id": 102, "customer_id": 202}
        ]
        self.session.table.return_value.collect.return_value = interactions_data

        # Test the validation function
        result = validate_interactions_table(self.session)
        self.assertTrue(result)

    def test_validate_outcomes_table(self):
        # Mock the outcomes table data
        outcomes_data = [
            {"outcome_id": 401, "description": "resolved"},
            {"outcome_id": 402, "description": "unresolved"}
        ]
        self.session.table.return_value.collect.return_value = outcomes_data

        # Test the validation function
        result = validate_outcomes_table(self.session)
        self.assertTrue(result)

if __name__ == '__main__':
    unittest.main()