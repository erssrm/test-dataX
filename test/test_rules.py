import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import col

# Initialize Spark session
@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local").appName("unit tests").getOrCreate()

# Sample data for testing
@pytest.fixture
def sample_data(spark):
    schema = StructType([
        StructField("call_center_id", StringType(), True),
        StructField("location", StringType(), True),
        StructField("number_of_calls", IntegerType(), True),
        StructField("average_call_duration", FloatType(), True),
        StructField("call_timestamp", StringType(), True),
        StructField("customer_satisfaction", IntegerType(), True)
    ])
    data = [
        ("1", "New York", 100, 5.5, "2023-09-30 12:00:00", 4),
        ("2", "Los Angeles", -10, 3.2, "2023-10-02 14:00:00", 2),
        ("3", "Invalid Location", 50, 4.0, "2023-09-29 16:00:00", 5),
        ("1", "Chicago", 80, 6.0, "2023-09-28 10:00:00", 3)
    ]
    return spark.createDataFrame(data, schema)

def test_check_completeness(sample_data):
    from rules import check_completeness
    completeness_issues = check_completeness(sample_data)
    assert completeness_issues.filter((col("call_center_id") > 0) | (col("location") > 0)).count() == 0

def test_validate_location(sample_data):
    from rules import validate_location
    valid_locations = ["New York", "Los Angeles", "Chicago"]
    location_issues = validate_location(sample_data, valid_locations)
    assert location_issues.count() == 1

def test_validate_numeric_ranges(sample_data):
    from rules import validate_numeric_ranges
    numeric_issues = validate_numeric_ranges(sample_data)
    assert numeric_issues.count() == 1

def test_check_consistency(sample_data):
    from rules import check_consistency
    consistency_issues = check_consistency(sample_data)
    assert consistency_issues.count() == 0

def test_check_timeliness(sample_data):
    from rules import check_timeliness
    timeliness_issues = check_timeliness(sample_data)
    assert timeliness_issues.count() == 1

def test_check_uniqueness(sample_data):
    from rules import check_uniqueness
    uniqueness_issues = check_uniqueness(sample_data)
    assert uniqueness_issues.count() == 1

def test_validate_business_rules(sample_data):
    from rules import validate_business_rules
    business_rule_issues = validate_business_rules(sample_data)
    assert business_rule_issues.count() == 1