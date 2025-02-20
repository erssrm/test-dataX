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
        self.call_details_data = [
            (1, "2023-10-01 10:00:00", 300, 101, 201, "inbound", "completed"),
            (2, "2023-10-01 11:00:00", 200, 102, 202, "outbound", "missed"),
            (3, None, 150, 103, 203, "inbound", "completed"),  # Incomplete record
            (4, "2023-10-01 12:00:00", 250, 104, 204, "invalid_type", "completed")  # Invalid call type
        ]
        self.call_details_df = self.spark.createDataFrame(self.call_details_data, 
                                                          ["call_id", "timestamp", "duration", "agent_id", "customer_id", "call_type", "call_outcome"])

    def test_check_completeness(self):
        from pyspark.sql.functions import lit, when
        df = self.call_details_df.withColumn("is_complete", 
                                             when(col("call_id").isNotNull() & 
                                                  col("timestamp").isNotNull() & 
                                                  col("duration").isNotNull() & 
                                                  col("agent_id").isNotNull() & 
                                                  col("customer_id").isNotNull() & 
                                                  col("call_type").isNotNull() & 
                                                  col("call_outcome").isNotNull(), lit(True)).otherwise(lit(False)))
        result = df.filter(col("is_complete") == False).count()
        self.assertEqual(result, 1, "There should be 1 incomplete record")

    def test_check_valid_call_type(self):
        valid_call_types = ["inbound", "outbound"]
        df = self.call_details_df.withColumn("is_valid_call_type", 
                                             when(col("call_type").isin(valid_call_types), lit(True)).otherwise(lit(False)))
        result = df.filter(col("is_valid_call_type") == False).count()
        self.assertEqual(result, 1, "There should be 1 record with an invalid call type")

    def test_check_uniqueness(self):
        df = self.call_details_df.dropDuplicates(["call_id"])
        result = df.count()
        self.assertEqual(result, len(self.call_details_data), "All call_id values should be unique")

if __name__ == '__main__':
    unittest.main()