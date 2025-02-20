import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session for testing
@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local").appName("pytest").getOrCreate()

# Sample data for testing
@pytest.fixture(scope="module")
def sample_data(spark):
    call_records_data = [("1", "inbound", "completed"), ("2", "outbound", "failed"), ("3", "inbound", "no_answer")]
    agent_data = [("1", "morning"), ("2", "afternoon"), ("3", "night")]
    customer_data = [("1", "+1234567890"), ("2", "+1987654321"), ("3", "+1123456789")]
    call_queue_data = [("1", 5), ("2", 10), ("3", 0)]
    call_disposition_data = [("1", "completed"), ("2", "failed"), ("3", "no_answer")]

    call_records_df = spark.createDataFrame(call_records_data, ["call_id", "call_type", "call_outcome"])
    agent_df = spark.createDataFrame(agent_data, ["agent_id", "shift"])
    customer_df = spark.createDataFrame(customer_data, ["customer_id", "contact_info"])
    call_queue_df = spark.createDataFrame(call_queue_data, ["queue_id", "wait_time"])
    call_disposition_df = spark.createDataFrame(call_disposition_data, ["call_id", "disposition_code"])

    return call_records_df, agent_df, customer_df, call_queue_df, call_disposition_df

def test_call_records_quality(spark, sample_data):
    call_records_df, _, _, _, _ = sample_data
    call_records_df = call_records_df.withColumn("is_valid_call_type", col("call_type").isin("inbound", "outbound"))
    assert call_records_df.filter(col("is_valid_call_type") == False).count() == 0

def test_agent_quality(spark, sample_data):
    _, agent_df, _, _, _ = sample_data
    agent_df = agent_df.withColumn("is_valid_shift", col("shift").isin("morning", "afternoon", "night"))
    assert agent_df.filter(col("is_valid_shift") == False).count() == 0

def test_customer_quality(spark, sample_data):
    _, _, customer_df, _, _ = sample_data
    customer_df = customer_df.withColumn("is_valid_contact", col("contact_info").rlike(r"^\+?[1-9]\d{1,14}$"))
    assert customer_df.filter(col("is_valid_contact") == False).count() == 0

def test_call_queue_quality(spark, sample_data):
    _, _, _, call_queue_df, _ = sample_data
    call_queue_df = call_queue_df.withColumn("is_valid_wait_time", col("wait_time") >= 0)
    assert call_queue_df.filter(col("is_valid_wait_time") == False).count() == 0

def test_call_disposition_quality(spark, sample_data):
    _, _, _, _, call_disposition_df = sample_data
    call_disposition_df = call_disposition_df.withColumn("is_valid_disposition_code", col("disposition_code").isin("completed", "failed", "no_answer"))
    assert call_disposition_df.filter(col("is_valid_disposition_code") == False).count() == 0