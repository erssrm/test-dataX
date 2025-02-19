import unittest
from unittest.mock import MagicMock
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from your_module import check_completeness, check_accuracy, check_format, check_consistency, check_uniqueness, check_timeliness

class TestDataQualityFunctions(unittest.TestCase):

    def setUp(self):
        # Mock session and dataframes
        self.session = MagicMock(spec=Session)
        self.df = MagicMock()
        self.related_df = MagicMock()

    def test_check_completeness(self):
        # Test completeness check
        required_fields = ["field1", "field2"]
        result_df = check_completeness(self.df, required_fields)
        for field in required_fields:
            self.assertTrue(result_df.with_column.called_with(f"{field}_is_complete", ~col(field).is_null()))

    def test_check_accuracy(self):
        # Test accuracy check
        field = "field"
        valid_values = ["value1", "value2"]
        result_df = check_accuracy(self.df, field, valid_values)
        self.assertTrue(result_df.with_column.called_with(f"{field}_is_accurate", col(field).isin(valid_values)))

    def test_check_format(self):
        # Test format check
        field = "field"
        format_regex = r"^\d{4}-\d{2}-\d{2}$"
        result_df = check_format(self.df, field, format_regex)
        self.assertTrue(result_df.with_column.called_with(f"{field}_is_formatted", col(field).rlike(format_regex)))

    def test_check_consistency(self):
        # Test consistency check
        field = "field"
        related_field = "related_field"
        result_df = check_consistency(self.df, field, self.related_df, related_field)
        self.assertTrue(result_df.join.called_with(self.related_df, self.df[field] == self.related_df[related_field], "left_anti"))

    def test_check_uniqueness(self):
        # Test uniqueness check
        field = "field"
        result_df = check_uniqueness(self.df, field)
        self.assertTrue(result_df.group_by.called_with(field))
        self.assertTrue(result_df.count.called)
        self.assertTrue(result_df.filter.called_with(col("count") > 1))

    def test_check_timeliness(self):
        # Test timeliness check
        field = "field"
        max_date = "2023-12-31"
        result_df = check_timeliness(self.df, field, max_date)
        self.assertTrue(result_df.with_column.called_with(f"{field}_is_timely", col(field) <= max_date))

if __name__ == '__main__':
    unittest.main()