import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import Row

class TestDataQualityRules(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").appName("TestDataQuality").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_check_completeness(self):
        data = [Row(call_id=1, timestamp="2023-10-01", duration=300, agent_id=101, customer_id=201, call_type="inbound", call_outcome="completed"),
                Row(call_id=2, timestamp=None, duration=200, agent_id=102, customer_id=202, call_type="outbound", call_outcome="missed")]
        df = self.spark.createDataFrame(data)
        result_df = check_completeness(df, ["timestamp", "duration"])
        completeness_col = result_df.select("timestamp_completeness").collect()
        self.assertEqual(completeness_col[0][0], "Complete")
        self.assertEqual(completeness_col[1][0], "Incomplete")

    def test_check_uniqueness(self):
        data = [Row(call_id=1), Row(call_id=2), Row(call_id=1)]
        df = self.spark.createDataFrame(data)
        result_df = check_uniqueness(df, "call_id")
        uniqueness_col = result_df.select("call_id_uniqueness").collect()
        self.assertEqual(uniqueness_col[0][0], "Duplicate")
        self.assertEqual(uniqueness_col[1][0], "Unique")
        self.assertEqual(uniqueness_col[2][0], "Duplicate")

    def test_check_accuracy(self):
        data = [Row(call_type="inbound"), Row(call_type="outbound"), Row(call_type="invalid")]
        df = self.spark.createDataFrame(data)
        result_df = check_accuracy(df, "call_type", "^(inbound|outbound)$")
        accuracy_col = result_df.select("call_type_accuracy").collect()
        self.assertEqual(accuracy_col[0][0], "Valid")
        self.assertEqual(accuracy_col[1][0], "Valid")
        self.assertEqual(accuracy_col[2][0], "Invalid")

    def test_check_consistency(self):
        data = [Row(call_id=1), Row(call_id=2)]
        reference_data = [Row(call_id=1)]
        df = self.spark.createDataFrame(data)
        reference_df = self.spark.createDataFrame(reference_data)
        result_df = check_consistency(df, "call_id", reference_df, "call_id")
        consistency_col = result_df.select("call_id_consistency").collect()
        self.assertEqual(consistency_col[0][0], "Consistent")
        self.assertEqual(consistency_col[1][0], "Inconsistent")

    def test_check_timeliness(self):
        data = [Row(timestamp="2023-10-01"), Row(timestamp="2022-01-01")]
        df = self.spark.createDataFrame(data)
        result_df = check_timeliness(df, "timestamp", "2023-01-01", "2023-12-31")
        timeliness_col = result_df.select("timestamp_timeliness").collect()
        self.assertEqual(timeliness_col[0][0], "Timely")
        self.assertEqual(timeliness_col[1][0], "Outdated")

if __name__ == '__main__':
    unittest.main()