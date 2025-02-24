import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

class TestCallCenterDataQuality(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("TestCallCenterDataQuality").getOrCreate()
        cls.call_records_df = cls.spark.createDataFrame([
            (1, "2023-10-01 10:00:00", 300, "1234567890", "inbound"),
            (2, "2023-10-01 11:00:00", 200, "0987654321", "outbound"),
            (3, None, 150, "1122334455", "inbound")
        ], ["call_id", "timestamp", "duration", "caller_id", "call_type"])

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_check_completeness(self):
        completeness_df = self.call_records_df.select([
            col(c).isNull().alias(c) for c in ["call_id", "timestamp", "duration", "caller_id"]
        ])
        completeness_count = completeness_df.filter(completeness_df.timestamp).count()
        self.assertEqual(completeness_count, 1, "There should be 1 incomplete record for timestamp")

    def test_check_uniqueness(self):
        uniqueness_df = self.call_records_df.groupBy("call_id").count().filter(col("count") > 1)
        self.assertEqual(uniqueness_df.count(), 0, "All call_id values should be unique")

    def test_check_valid_values(self):
        valid_values_df = self.call_records_df.filter(~col("call_type").isin(["inbound", "outbound"]))
        self.assertEqual(valid_values_df.count(), 0, "All call_type values should be valid")

    def test_check_referential_integrity(self):
        # Assuming a reference DataFrame for testing
        ref_df = self.spark.createDataFrame([
            (1,), (2,), (4,)
        ], ["call_id"])
        integrity_df = self.call_records_df.join(ref_df, self.call_records_df.call_id == ref_df.call_id, "left_anti")
        self.assertEqual(integrity_df.count(), 1, "There should be 1 record with missing referential integrity")

if __name__ == '__main__':
    unittest.main()