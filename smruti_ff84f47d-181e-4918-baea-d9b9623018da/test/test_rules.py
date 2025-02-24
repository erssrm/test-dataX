import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

class TestDataQualityChecks(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").appName("TestCallCenterDataQuality").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        # Sample data for testing
        self.calls_data = [
            (1, "2023-10-01 10:00:00", 300, 101, 201, "inbound", "completed"),
            (2, "2023-10-01 11:00:00", 200, 102, 202, "outbound", "failed"),
            (3, None, 150, 103, 203, "inbound", "completed")  # Incomplete record
        ]
        self.calls_df = self.spark.createDataFrame(self.calls_data, ["call_id", "timestamp", "duration", "agent_id", "customer_id", "call_type", "call_outcome"])

        self.agents_data = [
            (101, "Alice", "morning", "Team A"),
            (102, "Bob", "afternoon", "Team B"),
            (103, "Charlie", "night", "Team C")
        ]
        self.agents_df = self.spark.createDataFrame(self.agents_data, ["agent_id", "name", "shift", "team"])

    def test_check_completeness(self):
        # Test completeness check
        completeness_result = self.calls_df.select([col(c).isNull().alias(c) for c in self.calls_df.columns])
        incomplete_records = completeness_result.filter(completeness_result.timestamp).count()
        self.assertEqual(incomplete_records, 1, "There should be 1 incomplete record")

    def test_check_uniqueness(self):
        # Test uniqueness check
        duplicate_calls_data = self.calls_data + [(1, "2023-10-01 10:00:00", 300, 101, 201, "inbound", "completed")]
        duplicate_calls_df = self.spark.createDataFrame(duplicate_calls_data, ["call_id", "timestamp", "duration", "agent_id", "customer_id", "call_type", "call_outcome"])
        duplicate_count = duplicate_calls_df.groupBy("call_id").count().filter("count > 1").count()
        self.assertEqual(duplicate_count, 1, "There should be 1 duplicate call_id")

    def test_check_valid_values(self):
        # Test valid values check
        invalid_calls_data = self.calls_data + [(4, "2023-10-01 12:00:00", 250, 104, 204, "invalid_type", "completed")]
        invalid_calls_df = self.spark.createDataFrame(invalid_calls_data, ["call_id", "timestamp", "duration", "agent_id", "customer_id", "call_type", "call_outcome"])
        invalid_values_count = invalid_calls_df.filter(~col("call_type").isin(["inbound", "outbound"])).count()
        self.assertEqual(invalid_values_count, 1, "There should be 1 record with invalid call_type")

    def test_check_referential_integrity(self):
        # Test referential integrity check
        missing_agent_calls_data = self.calls_data + [(5, "2023-10-01 13:00:00", 180, 999, 205, "inbound", "completed")]
        missing_agent_calls_df = self.spark.createDataFrame(missing_agent_calls_data, ["call_id", "timestamp", "duration", "agent_id", "customer_id", "call_type", "call_outcome"])
        missing_agents_count = missing_agent_calls_df.join(self.agents_df, missing_agent_calls_df.agent_id == self.agents_df.agent_id, "left_anti").count()
        self.assertEqual(missing_agents_count, 1, "There should be 1 record with missing agent reference")

if __name__ == '__main__':
    unittest.main()