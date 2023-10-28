import datetime
import logging
import time
from typing import List

import boto3
import requests
import xml.etree.ElementTree as ET

DEBUG = False


logging.basicConfig(
    filename='arxiv_fetch_daily_summaries.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def lambda_handler(event: dict, context) -> dict:
    """Main function to fetch arXiv summaries and upload them to S3.

    Args:
        event (dict): Event data containing parameters.
        context: AWS Lambda context.

    Returns:
        dict: Response with status and message.
    """
    logging.info(f"Received event: {event}")
    logging.info("Starting to fetch arXiv daily summaries")
    base_url = event.get("base_url")
    bucket_name = event.get("bucket_name")
    from_date = event.get("from_date")
    summary_set = event.get("summary_set")

    full_xml_responses = fetch_data(base_url, from_date, summary_set)
    upload_to_s3(bucket_name, from_date, summary_set, full_xml_responses)

    if DEBUG:
        with open(f'test_data/test{datetime.datetime.now()}.xml', 'w') as f:
            f.write(full_xml_responses[0])

    return {
        "statusCode": 200,
        "body": f"Successfully fetched arXiv daily summaries from {from_date}"
    }


def fetch_data(base_url: str, from_date: str, summary_set: str) -> List[str]:
    """Fetches XML summaries from arXiv.

    Args:
        base_url (str): Base URL for fetching data.
        from_date (str): Date for summaries.
        summary_set (str): Summary set to fetch.

    Returns:
        List[str]: List of fetched XML summaries.
    """
    full_xml_responses = []
    params = {'verb': 'ListRecords', 'set': summary_set, 'metadataPrefix': 'oai_dc', 'from': from_date}
    logging.info(f"Request parameters: {params}")
    while True:
        response = fetch_http_response(base_url, params)
        if response.status_code != 200:
            logging.error(f"HTTP error, probably told to back off: {response.status_code}")
            backoff_time = handle_http_error(response)
            if backoff_time:
                time.sleep(backoff_time)
                continue
            else:
                break

        xml_content = response.text
        full_xml_responses.append(xml_content)

        resumption_token = extract_resumption_token(xml_content)
        if resumption_token:
            logging.info(f"Resumption token: {resumption_token}")
            params = {'verb': 'ListRecords', 'resumptionToken': resumption_token}
            time.sleep(5)
        else:
            break

    return full_xml_responses


def fetch_http_response(base_url: str, params: dict) -> requests.Response:
    """Fetches HTTP response.

    Args:
        base_url (str): Base URL for the API.
        params (dict): Request parameters.

    Returns:
        requests.Response: Response object.
    """
    return requests.get(base_url, params=params)


def handle_http_error(response: requests.Response) -> int:
    """Handles HTTP errors.

    Args:
        response (requests.Response): HTTP response.

    Returns:
        int: Backoff time if needed, otherwise 0.
    """
    backoff_times = [30, 120]
    if response.status_code == 503:
        logging.info(f"Received 503, retrying after {backoff_times[0]} seconds")
        return response.headers.get('Retry-After', backoff_times.pop(0) if backoff_times else 30)
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


if __name__ == "__main__":
    event = {
        "base_url": "http://export.arxiv.org/oai2",
        "bucket_name": "techcraftingai-inbound-data",
        "from_date": "2023-10-24",
        "summary_set": "cs",
    }
    DEBUG = True
    lambda_handler(event, None)
