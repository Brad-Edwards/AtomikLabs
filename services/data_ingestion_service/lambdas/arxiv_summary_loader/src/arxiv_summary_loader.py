import json
import boto3
import psycopg2
from psycopg2 import sql
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context) -> dict:
    """
    This lambda function is triggered by an S3 event. It reads the S3 file content,
    parses the JSON, and inserts the data into the PostgreSQL database.

    Args:
        event (dict): S3 event details
        context (dict): Lambda context

    Returns:
        dict: Lambda response

    Raises:
        Exception: If there is an error processing the lambda function
    """
    try:
        bucket, key = get_s3_event_details(event)
        content = get_s3_file_content(bucket, key)
        data = json.loads(content)
        process_records(data["records"])
    except Exception as e:
        logger.error(f"Error processing lambda function: {str(e)}")
        raise

    return {"statusCode": 200, "body": json.dumps("Success")}


def get_s3_event_details(event) -> tuple:
    """
    Extracts the S3 bucket and key from the S3 event.

    Args:
        event (dict): S3 event details

    Returns:
        tuple: S3 bucket and key

    Raises:
        Exception: If there is an error getting the S3 event details
    """
    try:
        bucket = event["Records"][0]["s3"]["bucket"]["name"]
        key = event["Records"][0]["s3"]["object"]["key"]
        return bucket, key
    except Exception as e:
        logger.error(f"Error getting S3 event details: {str(e)}")
        raise


def get_s3_file_content(bucket, key) -> str:
    """
    Reads the S3 file content.

    Args:
        bucket (str): S3 bucket name
        key (str): S3 object key

    Returns:
        str: S3 file content

    Raises:
        Exception: If there is an error getting the S3 file content
    """
    try:
        s3 = boto3.client("s3")
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response["Body"].read().decode("utf-8")
        return content
    except Exception as e:
        logger.error(
            f"Error getting S3 file content from bucket {bucket}, key {key}: {str(e)}"
        )
        raise


def process_records(records):
    """
    Processes the records and inserts them into the PostgreSQL database.

    Args:
        records (list): List of records to process

    Raises:
        Exception: If there is an error processing the records
    """
    try:
        conn = get_postgresql_connection()
        cursor = conn.cursor()
        for record in records:
            process_record(record, cursor)
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error processing records: {str(e)}")
        raise


def get_postgresql_connection():
    """
    Gets a connection to the PostgreSQL database.

    Returns:
        psycopg2.connection: PostgreSQL connection
    """
    try:
        return psycopg2.connect(
            host=os.environ.get("DATABASE_HOST"),
            port=os.environ.get("DATABASE_PORT"),
            user=os.environ.get("DATABASE_USER"),
            password=os.environ.get("DATABASE_PASSWORD"),
            dbname=os.environ.get("DATABASE_NAME"),
            sslmode=os.environ.get("DATABASE_SSL_MODE"),
        )
    except Exception as e:
        logger.error(f"Error getting PostgreSQL connection: {str(e)}")
        raise


def get_category_id(category_name, cursor):
    """
    Gets the category ID from the database.

    Args:
        category_name (str): Category name
        cursor (psycopg2.cursor): PostgreSQL cursor

    Returns:
        int: Category ID
    """
    if category_name == "Unknown":
        return None
    cursor.execute(
        "SELECT category_id FROM categories WHERE abbreviation = %s", (category_name,)
    )
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        return None


def get_group_id(group_name, cursor):
    if group_name == "Unknown":
        return None
    cursor.execute("SELECT group_id FROM groups WHERE label = %s", (group_name,))
    result = cursor.fetchone()
    return result[0] if result else None


def process_record(record, cursor):
    """
    Processes a single record and inserts it into the PostgreSQL database.

    Args:
        record (dict): Record to process
        cursor (psycopg2.cursor): PostgreSQL cursor

    Raises:
        Exception: If there is an error processing the record
    """
    cursor.execute("BEGIN")

    primary_category_id = get_category_id(record["primary_category"], cursor)
    primary_group_id = get_group_id(record["primary_group"], cursor)

    cursor.execute(
        """
        INSERT INTO research (unique_identifier, abstract_url, full_text_url, abstract, title, date, primary_category, primary_group)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (unique_identifier) DO NOTHING RETURNING research_id
        """,
        (
            record["identifier"],
            record["abstract_url"],
            record["full_text_url"],
            record["abstract"],
            record["title"],
            record["date"],
            primary_category_id,
            primary_group_id,
        ),
    )

    research_result = cursor.fetchone()
    if research_result is None:
        cursor.execute(
            "SELECT research_id FROM research WHERE unique_identifier = %s",
            (record["identifier"],),
        )
        research_result = cursor.fetchone()
        if research_result is None:
            raise Exception("Research record not found and unable to insert.")
    research_id = research_result[0]

    for author in record["authors"]:
        cursor.execute(
            """
            INSERT INTO research_authors (last_name, first_name) VALUES (%s, %s)
            ON CONFLICT (last_name, first_name) DO NOTHING RETURNING author_id
            """,
            (author["last_name"], author["first_name"]),
        )
        author_result = cursor.fetchone()
        if author_result is None:
            cursor.execute(
                "SELECT author_id FROM research_authors WHERE last_name = %s AND first_name = %s",
                (author["last_name"], author["first_name"]),
            )
            author_result = cursor.fetchone()
            if author_result is None:
                raise Exception("Author record not found and unable to insert.")
        author_id = author_result[0]

        cursor.execute(
            """
            INSERT INTO research_author (research_id, author_id) VALUES (%s, %s)
            ON CONFLICT DO NOTHING
            """,
            (research_id, author_id),
        )

    for category in record["categories"]:
        cursor.execute(
            """
            SELECT category_id FROM categories WHERE name = %s
        """,
            (category,),
        )
        category_result = cursor.fetchone()
        if category_result:
            category_id = category_result[0]
            cursor.execute(
                """
                INSERT INTO research_categories (research_id, category_id) VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """,
                (research_id, category_id),
            )

    for group in record["groups"]:
        cursor.execute(
            """
            SELECT group_id FROM groups WHERE label = %s
        """,
            (group,),
        )
        group_result = cursor.fetchone()
        if group_result:
            group_id = group_result[0]
            cursor.execute(
                """
                INSERT INTO research_groups (research_id, group_id) VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """,
                (research_id, group_id),
            )

    cursor.execute("COMMIT")
