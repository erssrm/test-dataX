import unittest
from unittest.mock import MagicMock
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from your_module import (validate_call_records, validate_agent_information,
                         validate_customer_information, validate_call_queue,
                         validate_call_dispositions, validate_slas,
                         validate_feedback_surveys)

class TestValidationFunctions(unittest.TestCase):

    def setUp(self):
        # Mock the Snowflake session
        self.session = MagicMock(spec=Session)

    def test_validate_call_records(self):
        # Mock the data for call_records
        df = self.session.table.return_value
        df.with_column.return_value = df
        df.group_by.return_value.agg.return_value = df

        # Call the validation function
        result_df = validate_call_records(self.session)

        # Check if the function was called with the correct parameters
        self.session.table.assert_called_with("call_records")
        df.with_column.assert_called()
        df.group_by.assert_called_with("call_id")

    def test_validate_agent_information(self):
        # Mock the data for agent_information
        df = self.session.table.return_value
        df.with_column.return_value = df
        df.group_by.return_value.agg.return_value = df

        # Call the validation function
        result_df = validate_agent_information(self.session)

        # Check if the function was called with the correct parameters
        self.session.table.assert_called_with("agent_information")
        df.with_column.assert_called()
        df.group_by.assert_called_with("agent_id")

    def test_validate_customer_information(self):
        # Mock the data for customer_information
        df = self.session.table.return_value
        df.with_column.return_value = df
        df.group_by.return_value.agg.return_value = df

        # Call the validation function
        result_df = validate_customer_information(self.session)

        # Check if the function was called with the correct parameters
        self.session.table.assert_called_with("customer_information")
        df.with_column.assert_called()
        df.group_by.assert_called_with("customer_id")

    def test_validate_call_queue(self):
        # Mock the data for call_queue
        df = self.session.table.return_value
        df.with_column.return_value = df
        df.group_by.return_value.agg.return_value = df

        # Call the validation function
        result_df = validate_call_queue(self.session)

        # Check if the function was called with the correct parameters
        self.session.table.assert_called_with("call_queue")
        df.with_column.assert_called()
        df.group_by.assert_called_with("queue_id")

    def test_validate_call_dispositions(self):
        # Mock the data for call_dispositions
        df = self.session.table.return_value
        df.with_column.return_value = df
        df.group_by.return_value.agg.return_value = df

        # Call the validation function
        result_df = validate_call_dispositions(self.session)

        # Check if the function was called with the correct parameters
        self.session.table.assert_called_with("call_dispositions")
        df.with_column.assert_called()
        df.group_by.assert_called_with("disposition_id")

    def test_validate_slas(self):
        # Mock the data for slas
        df = self.session.table.return_value
        df.with_column.return_value = df
        df.group_by.return_value.agg.return_value = df

        # Call the validation function
        result_df = validate_slas(self.session)

        # Check if the function was called with the correct parameters
        self.session.table.assert_called_with("slas")
        df.with_column.assert_called()
        df.group_by.assert_called_with("sla_id")

    def test_validate_feedback_surveys(self):
        # Mock the data for feedback_surveys
        df = self.session.table.return_value
        df.with_column.return_value = df
        df.group_by.return_value.agg.return_value = df

        # Call the validation function
        result_df = validate_feedback_surveys(self.session)

        # Check if the function was called with the correct parameters
        self.session.table.assert_called_with("feedback_surveys")
        df.with_column.assert_called()
        df.group_by.assert_called_with("feedback_id")

if __name__ == '__main__':
    unittest.main()