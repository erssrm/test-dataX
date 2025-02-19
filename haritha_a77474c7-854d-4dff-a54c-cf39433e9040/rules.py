from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, is_null, lit

def validate_calls_table(session: Session):
    calls_df = session.table("calls")

    # Completeness: Ensure all required fields are filled
    completeness_checks = [
        calls_df.filter(is_null(col("call_id"))).count() == 0,
        calls_df.filter(is_null(col("timestamp"))).count() == 0,
        calls_df.filter(is_null(col("duration"))).count() == 0,
        calls_df.filter(is_null(col("agent_id"))).count() == 0,
        calls_df.filter(is_null(col("customer_id"))).count() == 0,
        calls_df.filter(is_null(col("call_type"))).count() == 0,
        calls_df.filter(is_null(col("outcome"))).count() == 0
    ]

    # Uniqueness: Ensure call ID is unique
    uniqueness_check = calls_df.select("call_id").distinct().count() == calls_df.count()

    return all(completeness_checks) and uniqueness_check

def validate_agents_table(session: Session):
    agents_df = session.table("agents")

    # Completeness: Ensure all required fields are filled
    completeness_checks = [
        agents_df.filter(is_null(col("agent_id"))).count() == 0,
        agents_df.filter(is_null(col("name"))).count() == 0,
        agents_df.filter(is_null(col("shift"))).count() == 0
    ]

    # Uniqueness: Ensure agent ID is unique
    uniqueness_check = agents_df.select("agent_id").distinct().count() == agents_df.count()

    return all(completeness_checks) and uniqueness_check

def validate_customers_table(session: Session):
    customers_df = session.table("customers")

    # Completeness: Ensure all required fields are filled
    completeness_checks = [
        customers_df.filter(is_null(col("customer_id"))).count() == 0,
        customers_df.filter(is_null(col("name"))).count() == 0,
        customers_df.filter(is_null(col("contact_details"))).count() == 0
    ]

    # Uniqueness: Ensure customer ID is unique
    uniqueness_check = customers_df.select("customer_id").distinct().count() == customers_df.count()

    return all(completeness_checks) and uniqueness_check

def validate_interactions_table(session: Session):
    interactions_df = session.table("interactions")

    # Completeness: Ensure all required fields are filled
    completeness_checks = [
        interactions_df.filter(is_null(col("interaction_id"))).count() == 0,
        interactions_df.filter(is_null(col("agent_id"))).count() == 0,
        interactions_df.filter(is_null(col("customer_id"))).count() == 0
    ]

    # Uniqueness: Ensure interaction ID is unique
    uniqueness_check = interactions_df.select("interaction_id").distinct().count() == interactions_df.count()

    return all(completeness_checks) and uniqueness_check

def validate_outcomes_table(session: Session):
    outcomes_df = session.table("outcomes")

    # Completeness: Ensure all required fields are filled
    completeness_checks = [
        outcomes_df.filter(is_null(col("outcome_id"))).count() == 0,
        outcomes_df.filter(is_null(col("description"))).count() == 0
    ]

    # Uniqueness: Ensure outcome ID is unique
    uniqueness_check = outcomes_df.select("outcome_id").distinct().count() == outcomes_df.count()

    return all(completeness_checks) and uniqueness_check