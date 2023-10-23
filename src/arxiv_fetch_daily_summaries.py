"""arxiv_fetch_daily_summaries.py"""

from datetime import datetime, timedelta
import json
import time
from typing import List, Optional, Tuple

import boto3
import requests
import xml.etree.ElementTree as ET


def lambda_handler(event: dict, context) -> dict:
    """
    Fetches arXiv daily summaries from the specified date and uploads them to S3.

    Args:
        event (dict): The event data passed to the function.
        context (LambdaContext): The context in which the function is called.

    Returns:
        dict: A dict with a status code and message.
    """
    base_url = event.get("base_url")
    bucket_name = event.get("bucket_name")
    summary_set = event.get("summary_set")

    yesterday = datetime.today() - timedelta(days=1)
    from_date = yesterday.strftime("%Y-%m-%d")
    day = yesterday.strftime("%a")

    full_xml_responses = fetch_arxiv_data(base_url, from_date, summary_set)
    upload_to_s3(bucket_name, from_date, summary_set, full_xml_responses)

    # Deal with variable availability of daily summaries on weekends
    # TODO: replace with DB query
    if day == "Sun":
        fri = yesterday - timedelta(days=1)
        fri_date = fri.strftime("%Y-%m-%d")
        fri_xml_responses = fetch_arxiv_data(base_url, fri_date, summary_set)
        upload_to_s3(bucket_name, fri_date, summary_set, fri_xml_responses)

    if day == "Mon":
        sat = yesterday - timedelta(days=2)
        sat_date = sat.strftime("%Y-%m-%d")
        sat_xml_responses = fetch_arxiv_data(base_url, sat_date, summary_set)
        upload_to_s3(bucket_name, sat_date, summary_set, sat_xml_responses)

        sun = yesterday - timedelta(days=1)
        sun_date = sun.strftime("%Y-%m-%d")
        sun_xml_responses = fetch_arxiv_data(base_url, sun_date, summary_set)
        upload_to_s3(bucket_name, sun_date, summary_set, sun_xml_responses)

    if day == "Tues":
        mon = yesterday - timedelta(days=3)
        mon_date = mon.strftime("%Y-%m-%d")
        mon_xml_responses = fetch_arxiv_data(base_url, mon_date, summary_set)
        upload_to_s3(bucket_name, mon_date, summary_set, mon_xml_responses)

        sat = yesterday - timedelta(days=2)
        sat_date = sat.strftime("%Y-%m-%d")
        sat_xml_responses = fetch_arxiv_data(base_url, sat_date, summary_set)
        upload_to_s3(bucket_name, sat_date, summary_set, sat_xml_responses)

        sun = yesterday - timedelta(days=1)
        sun_date = sun.strftime("%Y-%m-%d")
        sun_xml_responses = fetch_arxiv_data(base_url, sun_date, summary_set)
        upload_to_s3(bucket_name, sun_date, summary_set, sun_xml_responses)

    return {
        "statusCode": 200,
        "body": f"Successfully fetched arXiv daily summaries from {from_date}",
    }


def fetch_arxiv_data(base_url: str, from_date: str, summary_set: str) -> List[str]:
    """
    Fetches arXiv daily summaries from the specified date.

    Args:
        base_url (str): The base URL of the arXiv OAI-PMH endpoint.
        from_date (str): The date from which to fetch daily summaries.
        summary_set (str): The set of daily summaries to fetch.

    Returns:
        List[str]: A list of XML responses from the arXiv OAI-PMH endpoint.
    """
    params = initialize_params(from_date, summary_set)
    full_xml_responses = []

    while True:
        response, resumption_token = fetch_data_from_endpoint(base_url, params)
        if response:
            full_xml_responses.append(response)

        if resumption_token:
            params = {"verb": "ListRecords", "resumptionToken": resumption_token}
            time.sleep(5)
        else:
            break

    return full_xml_responses


def initialize_params(from_date: str, summary_set: str) -> dict:
    """
    Initializes the parameters for the arXiv OAI-PMH endpoint.

    Args:
        from_date (str): The date from which to fetch daily summaries.
        summary_set (str): The set of daily summaries to fetch.

    Returns:
        dict: A dict of parameters for the arXiv OAI-PMH endpoint.
    """
    return {
        "verb": "ListRecords",
        "set": summary_set,
        "metadataPrefix": "oai_dc",
        "from": from_date,
    }


def fetch_data_from_endpoint(
    base_url: str, params: dict
) -> Tuple[Optional[str], Optional[str]]:
    """
    Fetches data from the arXiv OAI-PMH endpoint.

    Args:
        base_url (str): The base URL of the arXiv OAI-PMH endpoint.
        params (dict): A dict of parameters for the arXiv OAI-PMH endpoint.

    Returns:
        Tuple[Optional[str], Optional[str]]: A tuple containing the XML response and resumption token.
    """
    backoff_times = [30, 120]
    resumption_token = None

    try:
        print(f"Fetching arxiv research summaries with parameters: {params}")
        response = requests.get(base_url, params=params)
        response.raise_for_status()

        root = ET.fromstring(response.content)
        resumption_token_element = find_resumption_token(root)

        if resumption_token_element:
            resumption_token = resumption_token_element.text
            print(f"Found resumptionToken: {resumption_token}")

        return response.text, resumption_token

    except requests.exceptions.HTTPError as e:
        handle_http_error(e, response, backoff_times)

    except Exception as e:
        print(f"An unexpected error occurred: {e}")

    return None, None


def find_resumption_token(root: ET.Element) -> Optional[ET.Element]:
    """
    Finds the resumption token in the XML response.

    Args:
        root (ET.Element): The root element of the XML response.

    Returns:
        Optional[ET.Element]: The resumption token element.
    """
    return root.find(".//{http://www.openarchives.org/OAI/2.0/}resumptionToken")


def handle_http_error(
    e: Exception, response: requests.Response, backoff_times: List[int]
):
    """
    Handles HTTP errors.

    Args:
        e (Exception): The exception that was raised.
        response (requests.Response): The response from the arXiv OAI-PMH endpoint.
        backoff_times (List[int]): A list of backoff times.

    Returns:
        None
    """
    print(f"HTTP error occurred: {e}")
    if response.status_code == 503:
        backoff_time = response.headers.get(
            "Retry-After", backoff_times.pop(0) if backoff_times else 30
        )
        print(f"Received 503 error, backing off for {backoff_time} seconds.")
        time.sleep(int(backoff_time))


def upload_to_s3(
    bucket_name: str, from_date: str, summary_set: str, full_xml_responses: List[str]
):
    """
    Uploads the XML responses to S3.

    Args:
        bucket_name (str): The name of the S3 bucket.
        from_date (str): The date from which to fetch daily summaries.
        summary_set (str): The set of daily summaries to fetch.
        full_xml_responses (List[str]): A list of XML responses from the arXiv OAI-PMH endpoint.

    Returns:
        None
    """
    s3 = boto3.client("s3")
    s3.put_object(
        Body=json.dumps(full_xml_responses),
        Bucket=bucket_name,
        Key=f"arxiv/{summary_set}-{from_date}.json",
    )
