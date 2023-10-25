import json
from collections import defaultdict
import xml.etree.ElementTree as ET

import boto3
import json


def lambda_handler(event: dict, context) -> dict:
    """
    Parses arXiv daily summaries from the specified date and uploads them to S3.

    Args:
        event (dict): The event data passed to the function.
        context (LambdaContext): The context in which the function is called.

    Returns:
        dict: A dict with a status code and message.
    """

    #xml_data = event.get("xml_data")
    #from_date = event.get("from_date")
    print(event)

    #extracted_data_chunk = parse_xml_data(xml_data, from_date)

    #return {
    #    "statusCode": 200,
    #    "body": f"Successfully parsed arXiv daily summaries from {from_date}",
    #    "extracted_data_chunk": extracted_data_chunk
    #}


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


def parse_xml_data(xml_data: str, from_date: str) -> dict:
    extracted_data_chunk = defaultdict(list)

    try:
        root = ET.fromstring(xml_data)
        ns = {
            'oai': 'http://www.openarchives.org/OAI/2.0/',
            'dc': 'http://purl.org/dc/elements/1.1/'
        }

        for record in root.findall(".//oai:record", ns):
            date_elements = record.findall(".//dc:date", ns)
            if len(date_elements) != 1:
                continue

            identifier = record.find(".//oai:identifier", ns).text
            abstract_url = record.find(".//dc:identifier", ns).text

            creators_elements = record.findall(".//dc:creator", ns)
            authors = []
            for creator in creators_elements:
                name_parts = creator.text.split(", ", 1)
                last_name = name_parts[0]
                first_name = name_parts[1] if len(name_parts) > 1 else ""
                authors.append({'last_name': last_name, 'first_name': first_name})

            subjects_elements = record.findall(".//dc:subject", ns)
            categories = [cs_categories_inverted.get(subject.text, "") for subject in subjects_elements]

            primary_category = categories[0] if categories else ""

            abstract = record.find(".//dc:description", ns).text
            title = record.find(".//dc:title", ns).text
            date = date_elements[0].text
            group = 'cs'

            extracted_data_chunk['records'].append({
                'identifier': identifier,
                'abstract_url': abstract_url,
                'authors': authors,
                'primary_category': primary_category,
                'categories': categories,  # All categories
                'abstract': abstract,
                'title': title,
                'date': date,
                'group': group
            })

    except ET.ParseError as e:
        print(f"Parse error: {e}")

    return extracted_data_chunk


if __name__ == "__main__":
    lambda_handler({
        "xml_data": "",
        "from_date": "2021-10-24"
    }, None)