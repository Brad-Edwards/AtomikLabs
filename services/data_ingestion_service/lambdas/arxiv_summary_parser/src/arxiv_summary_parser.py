""" This module is responsible for parsing arXiv daily summaries and extracting relevant data. """

from collections import defaultdict
import json
import logging
import os
from typing import List, Dict, Union

import boto3
import xml.etree.ElementTree as ET
from botocore.client import BaseClient
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

logging.getLogger().setLevel(logging.INFO)


def load_config() -> Dict[str, Union[str, Dict[str, str]]]:
    """
    Loads configuration.

    Args:
        config_path (str): The path to the configuration file.

    Returns:
        Dict[str, Union[str, Dict[str, str]]]: The configuration.
    """
    return {
        'cs_categories_inverted': {
            'Computer Science - Artifical Intelligence': 'AI',
            'Computer Science - Hardware Architecture': 'AR',
            'Computer Science - Computational Complexity': 'CC',
            'Computer Science - Computational Engineering, Finance, and Science': 'CE',
            'Computer Science - Computational Geometry': 'CG',
            'Computer Science - Computation and Language': 'CL',
            'Computer Science - Cryptography and Security': 'CR',
            'Computer Science - Computer Vision and Pattern Recognition': 'CV',
            'Computer Science - Computers and Society': 'CY',
            'Computer Science - Databases': 'DB',
            'Computer Science - Distributed, Parallel, and Cluster Computing': 'DC',
            'Computer Science - Digital Libraries': 'DL',
            'Computer Science - Discrete Mathematics': 'DM',
            'Computer Science - Data Structures and Algorithms': 'DS',
            'Computer Science - Emerging Technologies': 'ET',
            'Computer Science - Formal Languages and Automata Theory': 'FL',
            'Computer Science - General Literature': 'GL',
            'Computer Science - Graphics': 'GR',
            'Computer Science - Computer Science and Game Theory': 'GT',
            'Computer Science - Human-Computer Interaction': 'HC',
            'Computer Science - Information Retrieval': 'IR',
            'Computer Science - Information Theory': 'IT',
            'Computer Science - Machine Learning': 'LG',
            'Computer Science - Logic in Computer Science': 'LO',
            'Computer Science - Multiagent Systems': 'MA',
            'Computer Science - Multimedia': 'MM',
            'Computer Science - Mathematical Software': 'MS',
            'Computer Science - Numerical Analysis': 'NA',
            'Computer Science - Neural and Evolutionary Computing': 'NE',
            'Computer Science - Networking and Internet Architecture': 'NI',
            'Computer Science - Other Computer Science': 'OH',
            'Computer Science - Operating Systems': 'OS',
            'Computer Science - Performance': 'PF',
            'Computer Science - Programming Languages': 'PL',
            'Computer Science - Robotics': 'RO',
            'Computer Science - Symbolic Computation': 'SC',
            'Computer Science - Sound': 'SD',
            'Computer Science - Software Engineering': 'SE',
            'Computer Science - Social and Information Networks': 'SI',
            'Computer Science - Systems and Control': 'SY'
        },
        'bucket_name': os.environ.get('BUCKET_NAME'),
        'save_path': os.environ.get('SAVE_PATH'),
    }


CONFIG = load_config()


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
    logging.info("Starting to parse arXiv daily summaries")
    try:
        s3 = boto3.client("s3")
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        logging.info(f"Processing arXiv daily summaries for bucket: {bucket}, key: {key}")
    except KeyError as e:
        logging.error(f"Malformed event: {event}. Missing key: {e}")
        return {
            "statusCode": 400,
            "body": "Malformed event"
        }

    try:
        logging.info(f"Fetching S3 object for bucket: {bucket}, key: {key}")
        xml_data = fetch_s3_object(s3, bucket, key)
        logging.info(f"Fetched S3 object for bucket: {bucket}, key: {key}")
        extracted_data_chunk = parse_xml_data(xml_data)
        logging.info(f"Parsed XML data for bucket: {bucket}, key: {key}")
        upload_to_s3(s3, extracted_data_chunk, key)
    except Exception as e:
        logging.error(f"Error in Lambda handler: {str(e)}")
        return {
            "statusCode": 500,
            "body": "Failed to parse arXiv daily summaries"
        }

    logging.info("Successfully parsed arXiv daily summaries.")

    return {
        "statusCode": 200,
        "body": "Successfully parsed arXiv daily summaries"
    }


def fetch_s3_object(client: BaseClient, bucket: str, key: str) -> str:
    """
    Fetches an S3 object.

    Args:
        client (BaseClient): The S3 client.
        bucket (str): The bucket name.
        key (str): The key of the object.

    Returns:
        str: The object data.
    """
    response = fetch_raw_object(client, bucket, key)
    sanitized_data = sanitize_object_data(response['Body'].read().decode('utf-8').strip())
    return decode_s3_object(sanitized_data, key)


def fetch_raw_object(client: BaseClient, bucket: str, key: str) -> dict:
    """
    Fetches an S3 object.

    Args:
        client (BaseClient): The S3 client.
        bucket (str): The bucket name.
        key (str): The key of the object.

    Returns:
        dict: The object data.
    """
    try:
        return client.get_object(Bucket=bucket, Key=key)
    except (NoCredentialsError, PartialCredentialsError) as e:
        log_error("Credentials issue", str(e))
        raise
    except Exception as e:
        log_error(f"Error fetching S3 object for key {key}", str(e))
        raise


def log_error(message: str, error: str):
    """
    Logs an error.

    Args:
        message (str): The error message.
        error (str): The error.
    """
    logging.error(f"{message}: {error}")


def sanitize_object_data(raw_data: str) -> str:
    """
    Sanitizes object data.

    Args:
        raw_data (str): The raw data.

    Returns:
        str: The sanitized data.
    """
    return ''.join(ch for ch in raw_data if ch in {'\n', '\r'} or 32 <= ord(ch) <= 126)


def decode_s3_object(data_str: str, key: str) -> Union[str, List[str]]:
    """
    Decodes S3 object data.

    Args:
        data_str (str): The data string.
        key (str): The key of the object.

    Returns:
        Union[str, List[str]]: The decoded data.
    """
    if not data_str:
        logging.warning(f"Empty data received for key: {key}")
        return ""

    logging.info(f"First few characters of data for key {key}: {data_str[:50]}")
    return data_str


def upload_to_s3(client: BaseClient, data: dict, key: str) -> None:
    """
    Uploads data to S3.

    Args:
        client (BaseClient): The S3 client.
        data (dict): The data to upload.
        key (str): The key of the object.
    """
    if not data:
        logging.warning("No data to upload.")
        return

    save_path = CONFIG.get('save_path')
    if save_path is None:
        logging.error("save_path is None in config.")
        return

    bucket_name = CONFIG.get('bucket_name')
    object_name = f"{save_path}/{key.replace('arxiv/', '')}-parsed.json"

    try:
        client.put_object(
            Body=json.dumps(data),
            Bucket=bucket_name,
            Key=object_name,
            ContentType='application/json'
        )
    except Exception as e:
        logging.error(f"Failed to upload to S3: {e}")


def parse_xml_data(xml_data: str) -> dict:
    """
    Parses XML data.

    Args:
        xml_data (str): The XML data.

    Returns:
        dict: The parsed data.
    """
    if not validate_xml_data(xml_data):
        return {}

    root, ns = parse_and_get_root(xml_data)
    if root is None or not validate_namespaces(xml_data, ns):
        return {}

    return extract_data_from_records(root, ns)


def validate_xml_data(xml_data: str) -> bool:
    """
    Validates XML data.

    Args:
        xml_data (str): The XML data.

    Returns:
        bool: True if the data is valid, False otherwise.
    """
    if not xml_data:
        logging.warning("Received empty or None XML data.")
        return False
    return True


def parse_and_get_root(xml_data: str) -> tuple:
    """
    Parses XML data and returns the root and namespaces.

    Args:
        xml_data (str): The XML data.

    Returns:
        tuple: The root and namespaces.
    """
    try:
        root = ET.fromstring(xml_data)
    except ET.ParseError as e:
        logging.error(f"Failed to parse XML: {e}")
        return None, {}

    ns = {'oai': 'http://www.openarchives.org/OAI/2.0/', 'dc': 'http://purl.org/dc/elements/1.1/'}
    return root, ns


def validate_namespaces(xml_data: str, ns: dict) -> bool:
    """
    Validates namespaces.

    Args:
        xml_data (str): The XML data.
        ns (dict): The namespaces.

    Returns:
        bool: True if the namespaces are valid, False otherwise.
    """
    if not all(namespace in xml_data for namespace in ns.values()):
        logging.warning("Namespaces are not as expected.")
        return False
    return True


def extract_data_from_records(root, ns: dict) -> dict:
    """
    Extracts data from records.

    Args:
        root (ET.Element): The root element.
        ns (dict): A dict of namespaces.

    Returns:
        dict: A dict with extracted data.
    """
    extracted_data_chunk = defaultdict(list)
    records = root.findall(".//oai:record", ns)

    if not records:
        logging.warning("No records found in XML.")
        return {}

    for record in records:
        date_elements = record.findall(".//dc:date", ns)
        if len(date_elements) != 1:
            logging.info("Record skipped due to multiple or zero date elements.")
            continue
        extracted_data_chunk['records'].append(extract_record_data(record, ns))

    return extracted_data_chunk


def extract_record_data(record, ns: dict) -> dict:
    """
    Extracts relevant data from an arXiv research summary record.

    Args:
        record (ET.Element): The record element.
        ns (dict): A dict of namespaces.

    Returns:
        dict: A dict with extracted data.
    """
    identifier = record.find(".//oai:identifier", ns)
    abstract_url = record.find(".//dc:identifier", ns)
    authors = extract_authors(record, ns)
    categories = extract_categories(record, ns)
    primary_category = categories[0] if categories else ""
    abstract = record.find(".//dc:description", ns)
    title = record.find(".//dc:title", ns)
    date = record.find(".//dc:date", ns)

    if any(el is None for el in [identifier, abstract_url, abstract, title, date]):
        logging.warning("Missing essential elements in record. Skipping.")
        return {}

    return {
        'identifier': identifier.text,
        'abstract_url': abstract_url.text,
        'authors': authors,
        'primary_category': primary_category,
        'categories': categories,
        'abstract': abstract.text,
        'title': title.text,
        'date': date.text,
        'group': 'cs'
    }


def extract_authors(record, ns: dict) -> list:
    """
    Extracts authors from an arXiv research summary record.

    Args:
        record (ET.Element): The record element.
        ns (dict): A dict of namespaces.

    Returns:
        list: A list of authors.
    """
    creators_elements = record.findall(".//dc:creator", ns)
    return [{'last_name': name.text.split(", ", 1)[0],
             'first_name': name.text.split(", ", 1)[1] if len(name.text.split(", ", 1)) > 1 else ''}
            for name in creators_elements if name.text]


def extract_categories(record, ns: dict) -> list:
    """
    Extracts categories from an arXiv research summary record.

    Args:
        record (ET.Element): The record element.
        ns (dict): A dict of namespaces.

    Returns:
        list: A list of categories.
    """
    subjects_elements = record.findall(".//dc:subject", ns)
    cs_categories_inverted = CONFIG.get('cs_categories_inverted')
    if cs_categories_inverted is not None:
        return [cs_categories_inverted.get(subject.text, "") for subject in subjects_elements if subject.text is not None]
    else:
        logging.warning("cs_categories_inverted is not initialized.")
    return []