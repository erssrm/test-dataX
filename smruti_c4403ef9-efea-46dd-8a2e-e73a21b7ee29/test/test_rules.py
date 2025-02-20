import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

class TestCallCenterDataQuality(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("TestCallCenterDataQuality").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        # Sample data for testing
        self.call_records_data = [
            (1, "2023-10-01 10:00:00", 300, 101, 201, "inbound", "completed"),
            (2, "2023-10-01 11:00:00", 200, 102, 202, "outbound", "completed"),
            (3, None, 150, 103, 203, "inbound", "completed")  # Incomplete record
        ]
        self.call_records_df = self.spark.createDataFrame(self.call_records_data, ["call_id", "timestamp", "duration", "agent_id", "customer_id", "call_type", "call_outcome"])

    def test_completeness(self):
        # Test completeness rule
        complete_df = self.call_records_df.withColumn("is_complete", col("call_id").isNotNull() & col("timestamp").isNotNull() & col("duration").isNotNull() & col("agent_id").isNotNull() & col("customer_id").isNotNull() & col("call_type").isNotNull() & col("call_outcome").isNotNull())
        complete_records = complete_df.filter(col("is_complete") == True).count()
        self.assertEqual(complete_records, 2)

    def test_valid_call_type(self):
        # Test valid call type rule
        valid_call_type_df = self.call_records_df.withColumn("is_valid_call_type", col("call_type").isin("inbound", "outbound"))
        valid_call_types = valid_call_type_df.filter(col("is_valid_call_type") == True).count()
        self.assertEqual(valid_call_types, 3)

    def test_uniqueness(self):
        # Test uniqueness rule
        unique_call_ids = self.call_records_df.select("call_id").distinct().count()
        self.assertEqual(unique_call_ids, 3)

if __name__ == '__main__':
    unittest.main()