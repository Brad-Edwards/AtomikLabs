""" This module is responsible for parsing arXiv daily summaries and extracting relevant data. """

import datetime
import json
import logging
from collections import defaultdict
from typing import List, Dict, Union

import boto3
import xml.etree.ElementTree as ET
from botocore.client import BaseClient
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

DEBUG = False

logging.basicConfig(
    filename='arxiv_summary_parser.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def setup_logging():
    """
    Sets up logging.
    """
    logging.info("Starting arXiv summary parsing.")


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
        'bucket_name': 'techcraftingai-data-processing',
        'object_path': 'parsed_data/',
    }


config = load_config()


def fetch_s3_object(client: BaseClient, bucket: str, key: str) -> str:
    """
    Fetches an object from S3.

    Args:
        client (BaseClient): The S3 client.
        bucket (str): The name of the bucket.
        key (str): The key of the object.

    Returns:
        str: The object data.
    """
    try:
        response = client.get_object(Bucket=bucket, Key=key)
    except (NoCredentialsError, PartialCredentialsError) as e:
        logging.error(f"Credentials issue: {str(e)}")
        raise
    except Exception as e:
        logging.error(f"Error fetching S3 object for key {key}: {str(e)}")
        raise

    return decode_s3_object(response['Body'].read().decode('utf-8').strip(), key)


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

    return data_str


def upload_to_s3(client: BaseClient, data: dict, key: str) -> None:
    """
    Uploads data to S3.

    Args:
        client (BaseClient): The S3 client.
        data (dict): The data to upload.
        key (str): The key of the object.
    """
    bucket_name = "techcraftingai-data-processing"
    object_path = config.get('object_path')
    object_name = f"{object_path}/{key}-parsed.json"
    client.put_object(
        Body=json.dumps(data),
        Bucket=bucket_name,
        Key=object_name,
        ContentType='application/json'
    )


def extract_record_data(record, ns: dict) -> dict:
    """
    Extracts relevant data from an arXiv research summary record.

    Args:
        record (ET.Element): The record element.
        ns (dict): A dict of namespaces.

    Returns:
        dict: A dict with extracted data.
    """
    identifier = record.find(".//oai:identifier", ns).text
    abstract_url = record.find(".//dc:identifier", ns).text
    authors = extract_authors(record, ns)
    categories = extract_categories(record, ns)
    primary_category = categories[0] if categories else ""
    abstract = record.find(".//dc:description", ns).text
    title = record.find(".//dc:title", ns).text
    date = record.find(".//dc:date", ns).text

    return {
        'identifier': identifier,
        'abstract_url': abstract_url,
        'authors': authors,
        'primary_category': primary_category,
        'categories': categories,
        'abstract': abstract,
        'title': title,
        'date': date,
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
             'first_name': name.text.split(", ", 1)[1]}
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
    cs_categories_inverted = config.get('cs_categories_inverted')
    return [cs_categories_inverted.get(subject.text, "") for subject in subjects_elements]


def parse_xml_data(xml_data: str) -> dict:
    """
    Parses XML data and extracts relevant data.

    Args:
        xml_data (str): The XML data.

    Returns:
        dict: A dict with extracted data.
    """
    extracted_data_chunk = defaultdict(list)
    root = ET.fromstring(xml_data)
    ns = {'oai': 'http://www.openarchives.org/OAI/2.0/', 'dc': 'http://purl.org/dc/elements/1.1/'}

    for record in root.findall(".//oai:record", ns):
        date_elements = record.findall(".//dc:date", ns)
        if len(date_elements) != 1:
            continue
        extracted_data_chunk['records'].append(extract_record_data(record, ns))

    return extracted_data_chunk


def lambda_handler(event: dict, context) -> dict:
    """
    The main entry point for the Lambda function.

    Args:
        event (dict): The event data.
        context: The context data.

    Returns:
        dict: A dict with the status code and body.
    """
    setup_logging()
    s3 = boto3.client("s3")
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    logging.info(f"Processing arXiv daily summaries for key: {key}")
    try:
        xml_data = fetch_s3_object(s3, bucket, key)
        extracted_data_chunk = parse_xml_data(xml_data)
        upload_to_s3(s3, extracted_data_chunk, key)
    except Exception as e:
        logging.error(f"Error in Lambda handler: {str(e)}")
        return {
            "statusCode": 500,
            "body": "Failed to parse arXiv daily summaries"
        }

    logging.info("Successfully parsed arXiv daily summaries.")

    if DEBUG:
        with open(f'../test_data/test{datetime.datetime.now()}.json', 'w') as f:
            f.write(json.dumps(extracted_data_chunk))

    return {
        "statusCode": 200,
        "body": "Successfully parsed arXiv daily summaries"
    }


if __name__ == "__main__":
    DEBUG = True
    lambda_handler({'Records': [{'s3': {'bucket': {'name': 'techcraftingai-inbound-data'},
                                        'object': {'key': 'arxiv/cs-2023-10-20-3.xml'}}}]}, None)
