import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

class TestCallCenterDataQuality(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("TestCallCenterDataQuality").getOrCreate()
        cls.call_details_data = [
            (1, "2023-10-01 10:00:00", 300, 101, 201, "inbound", "resolved"),
            (2, "2023-10-01 11:00:00", 200, 102, 202, "outbound", "unresolved"),
            (3, None, 150, 103, 203, "inbound", "resolved"),
            (4, "2023-10-01 12:00:00", 400, 104, 204, "outbound", None)
        ]
        cls.call_details_df = cls.spark.createDataFrame(cls.call_details_data, 
                                                        ["call_id", "timestamp", "duration", "agent_id", "customer_id", "call_type", "call_outcome"])

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_check_completeness(self):
        required_columns = ["call_id", "timestamp", "duration", "agent_id", "customer_id", "call_type", "call_outcome"]
        result_df = check_completeness(self.call_details_df, required_columns)
        expected_completeness = [True, True, False, False]
        self.assertEqual(result_df.select("is_complete").rdd.flatMap(lambda x: x).collect(), expected_completeness)

    def test_check_valid_values(self):
        result_df = check_valid_values(self.call_details_df, "call_type", ["inbound", "outbound"])
        expected_validity = [True, True, True, True]
        self.assertEqual(result_df.select("is_valid_call_type").rdd.flatMap(lambda x: x).collect(), expected_validity)

    def test_check_timestamp_format(self):
        result_df = check_timestamp_format(self.call_details_df, "timestamp")
        expected_timestamp_validity = [True, True, False, True]
        self.assertEqual(result_df.select("is_valid_timestamp").rdd.flatMap(lambda x: x).collect(), expected_timestamp_validity)

    def test_check_uniqueness(self):
        result_df = check_uniqueness(self.call_details_df.withColumn("is_unique", lit(True)), "call_id")
        expected_uniqueness = [True, True, True, True]
        self.assertEqual(result_df.select("is_unique").rdd.flatMap(lambda x: x).collect(), expected_uniqueness)

if __name__ == '__main__':
    unittest.main()