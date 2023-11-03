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


def get_postgresql_connection() -> psycopg2.connection:
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


def process_record(record, cursor):
    """
    Processes a single record and inserts it into the PostgreSQL database.

    Args:
        record (dict): Record to process
        cursor (psycopg2.cursor): PostgreSQL cursor

    Raises:
        Exception: If there is an error processing the record
    """
    try:
        research_id = insert_research(record, cursor)
        for author in record.get("authors", []):
            author_id = insert_author(author, cursor)
            insert_research_author(research_id, author_id, cursor)
    except Exception as e:
        logger.error(f"Error processing record: {str(e)}")
        raise


def validate_record(record):
    """
    Validates that the record contains all required fields.

    Args:
        record (dict): Record to validate

    Raises:
        ValueError: If the record is missing any required fields
    """
    required_fields = [
        "title",
        "primary_category",
        "abstract",
        "date",
        "identifier",
        "abstract_url",
        "categories",
        "group",
    ]
    if not all(field in record for field in required_fields):
        raise ValueError("Missing required fields in input data")


def get_full_text_url(abstract_url) -> str:
    """
    Gets the full text URL from the abstract URL.

    Args:
        abstract_url (str): Abstract URL

    Returns:
        str: Full text URL

    Raises:
        ValueError: If the abstract URL is invalid
    """
    return abstract_url.replace("/abs/", "/pdf/")


def insert_research_entry(record, cursor):
    cursor.execute(
        sql.SQL(
            """
        INSERT INTO research (
            title, primary_category, summary, date, unique_identifier, 
            abstract_url, full_text_url, stored_pdf_url, 
            stored_full_text_url, categories
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (unique_identifier)
        DO UPDATE SET
            title = excluded.title,
            primary_category = excluded.primary_category,
            summary = excluded.summary,
            date = excluded.date,
            abstract_url = excluded.abstract_url,
            full_text_url = excluded.full_text_url,
            stored_pdf_url = excluded.stored_pdf_url,
            stored_full_text_url = excluded.stored_full_text_url
        RETURNING research_id
    """
        ),
        (
            record["title"],
            record["primary_category"],
            record["abstract"],
            record["date"],
            record["identifier"],
            record["abstract_url"],
            get_full_text_url(record["abstract_url"]),  # full_text_url
            None,  # stored_pdf_url
            None,  # stored_full_text_url
        ),
    )
    return cursor.fetchone()[0]


def insert_groups_and_associate(research_id, record, cursor):
    """
    Inserts the research groups and associates them with the research entry.

    Args:
        research_id (int): Research ID
        record (dict): Record to process
        cursor (psycopg2.cursor): PostgreSQL cursor

    Raises:
        Exception: If there is an error inserting the research groups
    """
    for group_name in record["group"]:
        cursor.execute(
            sql.SQL(
                """
            INSERT INTO research_groups (group_name)
            VALUES (%s)
            ON CONFLICT (group_name)
            DO NOTHING
            RETURNING group_id
        """
            ),
            (group_name,),
        )
        group_id = cursor.fetchone()[0]

        is_primary = group_name == record["primary_group"]
        cursor.execute(
            sql.SQL(
                """
            INSERT INTO research_group (research_id, group_id, is_primary)
            VALUES (%s, %s, %s)
            ON CONFLICT (research_id, group_id)
            DO NOTHING
        """
            ),
            (research_id, group_id, is_primary),
        )


def insert_research(record, cursor) -> int:
    """
    Inserts the research entry into the database.

    Args:
        record (dict): Record to process
        cursor (psycopg2.cursor): PostgreSQL cursor

    Returns:
        int: Research ID

    Raises:
        Exception: If there is an error inserting the research entry
    """
    try:
        validate_record(record)
        research_id = insert_research_entry(record, cursor)
        insert_groups_and_associate(research_id, record, cursor)
        # Insert categories and associate logic would go here, following the pattern above
        return research_id
    except Exception as e:
        logger.error(f"Error inserting research: {str(e)}")
        raise


def insert_author(author, cursor):
    """
    Inserts the author into the database.

    Args:
        author (dict): Author to insert
        cursor (psycopg2.cursor): PostgreSQL cursor

    Returns:
        int: Author ID

    Raises:
        Exception: If there is an error inserting the author
    """
    try:
        cursor.execute(
            sql.SQL(
                """
            INSERT INTO research_authors (first_name, last_name)
            VALUES (%s, %s)
            ON CONFLICT (first_name, last_name)
            DO NOTHING;
        """
            ),
            (author["first_name"], author["last_name"]),
        )
        cursor.execute(
            sql.SQL(
                """
            SELECT author_id
            FROM research_authors
            WHERE first_name = %s AND last_name = %s;
        """
            ),
            (author["first_name"], author["last_name"]),
        )
        return cursor.fetchone()[0]
    except Exception as e:
        logger.error(f"Error inserting author: {str(e)}")
        raise


def insert_research_author(research_id, author_id, cursor):
    """
    Associates the research entry with the author.

    Args:
        research_id (int): Research ID
        author_id (int): Author ID
        cursor (psycopg2.cursor): PostgreSQL cursor

    Raises:
        Exception: If there is an error inserting the research_author entry
    """
    try:
        cursor.execute(
            sql.SQL(
                """
            INSERT INTO research_author (research_id, author_id)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
        """
            ),
            (research_id, author_id),
        )
    except Exception as e:
        logger.error(f"Error inserting research_author: {str(e)}")
        raise
