from datetime import datetime, timedelta
import logging
import os
import re
import time
from typing import List

import boto3
import psycopg2
import requests
import xml.etree.ElementTree as ET


logging.getLogger().setLevel(logging.INFO)


def initialize_db() -> (psycopg2.extensions.connection, psycopg2.extensions.cursor):
    """
    Initializes database connection.

    Returns:
        psycopg2.extensions.connection: Database connection.
        psycopg2.extensions.cursor: Database cursor.
    """
    conn = psycopg2.connect(
        host=os.environ.get("DATABASE_HOST"),
        port=os.environ.get("DATABASE_PORT"),
        user=os.environ.get("DATABASE_USER"),
        password=os.environ.get("DATABASE_PASSWORD"),
        dbname=os.environ.get("DATABASE_NAME"),
        sslmode=os.environ.get("DATABASE_SSL_MODE"),
    )
    cursor = conn.cursor()
    return conn, cursor


def generate_date_list(start_date_str: str, end_date_str: str) -> List[str]:
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
    delta = end_date - start_date
    return [(start_date + timedelta(days=i)).strftime('%Y-%m-%d') for i in range((delta.days) + 1)]


def finalize_db(conn, cursor):
    """
    Finalizes database connection.

    Args:
        conn (psycopg2.extensions.connection): Database connection.
        cursor (psycopg2.extensions.cursor): Database cursor.
    """
    conn.commit()
    cursor.close()
    conn.close()


def process_fetch(base_url, from_date, summary_set, bucket_name, cursor, fetched_data) -> bool:
    pattern = r"</dc:description>\s+<dc:date>" + re.escape(from_date) + r"</dc:date>\s+<dc:type>text</dc:type>"
    success = any(re.search(pattern, xml) for xml in fetched_data)

    if success:
        upload_to_s3(bucket_name, from_date, summary_set, fetched_data)
        set_fetch_success(from_date, cursor)
    else:
        set_fetch_failure(from_date, cursor)

    return success


def calculate_from_date() -> str:
    """Calculates from date for fetching summaries.

    Returns:
        str: From date.
    """
    today = datetime.today()
    yesterday = today - timedelta(days=1)
    return yesterday.strftime("%Y-%m-%d")


def insert_fetch_status(date, cursor):
    """
    Inserts fetch status as 'pending' for the given date.

    Args:
        date (str): Date for which to insert fetch status.
        cursor: Database cursor.
    """
    cursor.execute(
        "INSERT INTO research_fetch_status (fetch_date, status) VALUES (%s, 'pending') ON CONFLICT (fetch_date) DO NOTHING",
        (date,)
    )


def set_fetch_success(date, cursor):
    """
    Sets fetch status as 'success' for the given date.

    Args:
        date (str): Date for which to set fetch status.
        cursor: Database cursor.
    """
    cursor.execute(
        "UPDATE research_fetch_status SET status = 'success' WHERE fetch_date = %s",
        (date,)
    )


def set_fetch_failure(date, cursor):
    """
    Sets fetch status as 'failure' for the given date.

    Args:
        date (str): Date for which to set fetch status.
        cursor: Database cursor.
    """
    cursor.execute(
        "UPDATE research_fetch_status SET status = 'failure', retry_count = retry_count + 1 WHERE fetch_date = %s",
        (date,)
    )


def get_earliest_unfetched_date(cursor, days=5) -> str:
    """
    Gets the earliest unfetched date.

    Args:
        cursor: Database cursor.
        days (int): Number of days to look back.

    Returns:
        str: Earliest unfetched date.
    """
    today = datetime.today()
    past_dates = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(1, days + 1)]

    try:
        cursor.execute(
            "SELECT fetch_date FROM research_fetch_status WHERE fetch_date = ANY(%s::DATE[]) AND status = 'success'",
            (past_dates,)
        )
        fetched_dates = [result[0].strftime("%Y-%m-%d") for result in cursor.fetchall()]
        unfetched_dates = list(set(past_dates) - set(fetched_dates))

        earliest_date = min(unfetched_dates) if unfetched_dates else None
    except Exception as e:
        logging.error(f"Database query failed: {str(e)}")
        earliest_date = None

    return earliest_date or (today - timedelta(days=1)).strftime("%Y-%m-%d")


def lambda_handler(event: dict, context) -> dict:
    """
    The main entry point for the Lambda function.

    Args:
        event (dict): The event data.
        context: The context data.

    Returns:
        dict: A dict with the status code and body.
    """
    logging.info(f"Received event: {event}")
    logging.info("Starting to fetch arXiv daily summaries")

    base_url = event.get("base_url")
    bucket_name = event.get("bucket_name")
    summary_set = event.get("summary_set")

    conn, cursor = initialize_db()

    today = calculate_from_date()
    insert_fetch_status(today, cursor)

    earliest_unfetched_date = get_earliest_unfetched_date(cursor)
    logging.info(f"Earliest unfetched date: {earliest_unfetched_date}")
    if earliest_unfetched_date:
        full_xml_responses = fetch_data(base_url, earliest_unfetched_date, summary_set)
        date_list = generate_date_list(earliest_unfetched_date, today)
        logging.info(f"Date list: {date_list}")

        for date_to_fetch in date_list:
            logging.info(f"Fetching for date: {date_to_fetch}")
            insert_fetch_status(date_to_fetch, cursor)
            success = process_fetch(base_url, date_to_fetch, summary_set, bucket_name, cursor, full_xml_responses)
            if success:
                logging.info(f"Fetch successful for date: {date_to_fetch}")
            else:
                logging.error(f"Fetch failed for date: {date_to_fetch}")
        else:
            logging.warning(f"No unfetched dates found")

    finalize_db(conn, cursor)

    return {
        "statusCode": 200,
        "body": f"Attempted fetch for date: {earliest_unfetched_date}"
    }


def schedule_for_later():
    future_time = datetime.utcnow() + timedelta(hours=5)

    cron_time = future_time.strftime('%M %H %d %m ? %Y')

    client = boto3.client('events')

    response = client.put_rule(
        Name='DynamicRule',
        ScheduleExpression=f'cron({cron_time})',
        State='ENABLED'
    )

    rule_arn = response['RuleArn']

    client.put_targets(
        Rule='DynamicRule',
        Targets=[
            {
                'Id': 'ArxivFetchDailySummaries',
                'Arn': os.environ.get('LAMBDA_ARN')
            }
        ]
    )


def fetch_data(base_url: str, from_date: str, summary_set: str) -> List[str]:
    """
    Fetches data from the API.

    Args:
        base_url (str): Base URL for the API.
        from_date (str): Summary date.
        summary_set (str): Summary set.

    Returns:
        List[str]: List of XML responses.
    """
    full_xml_responses = []
    params = {'verb': 'ListRecords', 'set': summary_set, 'metadataPrefix': 'oai_dc', 'from': from_date}
    retry_count = 0
    while True:
        status_code, xml_content = fetch_http_response(base_url, params)
        if status_code != 200:
            logging.error(f"HTTP error, probably told to back off: {status_code}")
            backoff_time = handle_http_error(status_code, xml_content, retry_count)
            if backoff_time:
                time.sleep(backoff_time)
                retry_count += 1
                continue
            else:
                break

        full_xml_responses.append(xml_content)

        resumption_token = extract_resumption_token(xml_content)
        if resumption_token:
            logging.info(f"Resumption token: {resumption_token}")
            params = {'verb': 'ListRecords', 'resumptionToken': resumption_token}
            time.sleep(5)
        else:
            break

    return full_xml_responses


def fetch_http_response(base_url: str, params: dict) -> tuple[int, str]:
    """Fetches HTTP response.

    Args:
        base_url (str): Base URL for the API.
        params (dict): Request parameters.

    Returns:
        requests.Response: Response object.
    """
    response = requests.get(base_url, params=params)
    return response.status_code, response.text


def handle_http_error(status_code: int, response_text: str, retry_count: int) -> int:
    """
    Handles HTTP error.

    Args:
        status_code (int): HTTP status code.
        response_text (str): Response text.
        retry_count (int): Retry count.

    Returns:
        int: Backoff time.
    """
    if "maintenance" in response_text.lower():
        schedule_for_later()
        return 0
    backoff_times = [30, 120]
    if status_code == 503 and retry_count < len(backoff_times):
        logging.info(f"Received 503, retrying after {backoff_times[retry_count]} seconds")
        return backoff_times[retry_count]
    return 0


def extract_resumption_token(xml_content: str) -> str:
    """Extracts resumption token from XML content.

    Args:
        xml_content (str): XML content.

    Returns:
        str: Resumption token.
    """
    root = ET.fromstring(xml_content)
    token_element = root.find(".//{http://www.openarchives.org/OAI/2.0/}resumptionToken")
    return token_element.text if token_element is not None else ''


def upload_to_s3(bucket_name: str, from_date: str, summary_set: str, full_xml_responses: List[str]):
    """Uploads XML responses to S3.

    Args:
        bucket_name (str): S3 bucket name.
        from_date (str): Summary date.
        summary_set (str): Summary set.
        full_xml_responses (List[str]): XML responses.
    """
    logging.info(f"Uploading {len(full_xml_responses)} XML responses to S3")
    s3 = boto3.client("s3")

    for idx, xml_response in enumerate(full_xml_responses):
        s3.put_object(
            Body=xml_response,
            Bucket=bucket_name,
            Key=f"arxiv/{summary_set}-{from_date}-{idx}.xml",
        )
