""" This module is responsible for parsing arXiv daily summaries and extracting relevant data. """

import json
from collections import defaultdict
import xml.etree.ElementTree as ET
import boto3

from package.botocore.client import BaseClient

cs_categories_inverted = {
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
}


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
    s3 = client

    response = s3.get_object(Bucket=bucket, Key=key)
    data_list = json.loads(response['Body'].read().decode('utf-8'))

    if isinstance(data_list, list):
        return "".join(data_list)

    return data_list


def parse_xml_data(xml_data: str) -> dict:
    """
    Parses XML data and extracts relevant data.

    Args:
        xml_data (str): XML data to parse.

    Returns:
        dict: A dict with parsed data.
    """
    extracted_data_chunk = defaultdict(list)

    try:
        root = ET.fromstring(xml_data)
        ns = {'oai': 'http://www.openarchives.org/OAI/2.0/', 'dc': 'http://purl.org/dc/elements/1.1/'}

        for record in root.findall(".//oai:record", ns):
            date_elements = record.findall(".//dc:date", ns)
            if len(date_elements) != 1:
                continue

            extracted_data_chunk['records'].append(extract_record_data(record, ns))

    except ET.ParseError as e:
        print(f"Parse error: {e}")

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
    authors = []
    creators_elements = record.findall(".//dc:creator", ns)

    for creator in creators_elements:
        name_parts = creator.text.split(", ", 1)
        last_name = name_parts[0]
        first_name = name_parts[1] if len(name_parts) > 1 else ""
        authors.append({'last_name': last_name, 'first_name': first_name})

    return authors


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
    return [cs_categories_inverted.get(subject.text, "") for subject in subjects_elements]


def upload_to_s3(client: BaseClient, data: dict, key: str) -> None:
    """
    Uploads data to S3.

    Args:
        client (BaseClient): The S3 client.
        data (dict): The data to upload.
        key (str): The key of the object.

    Returns:
        None
    """
    s3 = client
    bucket_name = "techcraftingai-data-processing"
    object_name = f"arxiv/{key}-parsed.json"
    s3.put_object(
        Body=json.dumps(data),
        Bucket=bucket_name,
        Key=object_name,
        ContentType='application/json'
    )


def lambda_handler(event: dict, context) -> dict:
    """
    The main entry point for the Lambda function.

    Args:
        event (dict): The event data.
        context: The context data.

    Returns:
        dict: A dict with the status code and body.
    """
    s3 = boto3.client("s3")
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    xml_data = fetch_s3_object(s3, bucket, key)
    extracted_data_chunk = parse_xml_data(xml_data)
    upload_to_s3(s3, extracted_data_chunk, key)

    return {
        "statusCode": 200,
        "body": "Successfully parsed arXiv daily summaries"
    }


if __name__ == "__main__":
    test_event = {
        "Records": [
            {
                "s3": {
                    "bucket": {
                        "name": "techcraftingai-inbound-data"
                    },
                    "object": {
                        "key": "arxiv/cs-2023-10-24.json"
                    }
                }
            }
        ]
    }
    lambda_handler(test_event, None)