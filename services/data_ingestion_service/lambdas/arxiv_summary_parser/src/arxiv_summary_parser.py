""" This module is responsible for parsing arXiv daily summaries and extracting relevant data. """

from collections import defaultdict
import json
import logging
import os
from typing import List, Dict, Union, Tuple

import boto3
import xml.etree.ElementTree as ET
from botocore.client import BaseClient
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

CATEGORIES = "categories"
LABEL = "label"
SEPARATOR = "separator"
QUERY_TERM = "query_term"

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
        "cs": {
            LABEL: "cs",
            SEPARATOR: ".",
            QUERY_TERM: "cs",
            CATEGORIES: {
                "Computer Science - Artificial Intelligence": "AI",
                "Computer Science - Hardware Architecture": "AR",
                "Computer Science - Computational Complexity": "CC",
                "Computer Science - Computational Engineering, Finance, and Science": "CE",
                "Computer Science - Computational Geometry": "CG",
                "Computer Science - Computation and Language": "CL",
                "Computer Science - Cryptography and Security": "CR",
                "Computer Science - Computer Vision and Pattern Recognition": "CV",
                "Computer Science - Computers and Society": "CY",
                "Computer Science - Databases": "DB",
                "Computer Science - Distributed, Parallel, and Cluster Computing": "DC",
                "Computer Science - Digital Libraries": "DL",
                "Computer Science - Discrete Mathematics": "DM",
                "Computer Science - Data Structures and Algorithms": "DS",
                "Computer Science - Emerging Technologies": "ET",
                "Computer Science - Formal Languages and Automata Theory": "FL",
                "Computer Science - General Literature": "GL",
                "Computer Science - Graphics": "GR",
                "Computer Science - Computer Science and Game Theory": "GT",
                "Computer Science - Human-Computer Interaction": "HC",
                "Computer Science - Information Retrieval": "IR",
                "Computer Science - Information Theory": "IT",
                "Computer Science - Machine Learning": "LG",
                "Computer Science - Logic in Computer Science": "LO",
                "Computer Science - Multiagent Systems": "MA",
                "Computer Science - Multimedia": "MM",
                "Computer Science - Mathematical Software": "MS",
                "Computer Science - Numerical Analysis": "NA",
                "Computer Science - Neural and Evolutionary Computing": "NE",
                "Computer Science - Networking and Internet Architecture": "NI",
                "Computer Science - Other Computer Science": "OH",
                "Computer Science - Operating Systems": "OS",
                "Computer Science - Performance": "PF",
                "Computer Science - Programming Languages": "PL",
                "Computer Science - Robotics": "RO",
                "Computer Science - Symbolic Computation": "SC",
                "Computer Science - Sound": "SD",
                "Computer Science - Software Engineering": "SE",
                "Computer Science - Social and Information Networks": "SI",
                "Computer Science - Systems and Control": "SY",
            },
        },
        "econ": {
            LABEL: "econ",
            SEPARATOR: ".",
            QUERY_TERM: "econ",
            CATEGORIES: {
                "Economics - Econometrics": "EM",
                "Economics - General Economics": "GN",
                "Economics - Theoretical Economics": "TH",
            },
        },
        "eess": {
            LABEL: "eess",
            SEPARATOR: ".",
            QUERY_TERM: "eess",
            CATEGORIES: {
                "Electrical Engineering and Systems Science - Audio and Speech Processing": "AS",
                "Electrical Engineering and Systems Science - Image and Video Processing": "IV",
                "Electrical Engineering and Systems Science - Signal Processing": "SP",
                "Electrical Engineering and Systems Science - Systems and Control": "SY",
            },
        },
        "math": {
            LABEL: "math",
            SEPARATOR: ".",
            QUERY_TERM: "math",
            CATEGORIES: {
                "Mathematics - Commutative Algebra": "AC",
                "Mathematics - Algebraic Geometry": "AG",
                "Mathematics - Analysis of PDEs": "AP",
                "Mathematics - Algebraic Topology": "AT",
                "Mathematics - Classical Analysis and ODEs": "CA",
                "Mathematics - Combinatorics": "CO",
                "Mathematics - Category Theory": "CT",
                "Mathematics - Complex Variables": "CV",
                "Mathematics - Differential Geometry": "DG",
                "Mathematics - Dynamical Systems": "DS",
                "Mathematics - Functional Analysis": "FA",
                "Mathematics - General Mathematics": "GM",
                "Mathematics - General Topology": "GN",
                "Mathematics - Group Theory": "GR",
                "Mathematics - Geometric Topology": "GT",
                "Mathematics - History and Overview": "HO",
                "Mathematics - Information Theory": "IT",
                "Mathematics - K-Theory and Homology": "KT",
                "Mathematics - Logic": "LO",
                "Mathematics - Metric Geometry": "MG",
                "Mathematics - Mathematical Physics": "MP",
                "Mathematics - Numerical Analysis": "NA",
                "Mathematics - Number Theory": "NT",
                "Mathematics - Operator Algebras": "OA",
                "Mathematics - Optimization and Control": "OC",
                "Mathematics - Probability": "PR",
                "Mathematics - Quantum Algebra": "QA",
                "Mathematics - Rings and Algebras": "RA",
                "Mathematics - Representation Theory": "RT",
                "Mathematics - Symplectic Geometry": "SG",
                "Mathematics - Spectral Theory": "SP",
                "Mathematics - Statistics Theory": "ST",
            },
        },
        "astro-ph": {
            LABEL: "astro-ph",
            SEPARATOR: ".",
            QUERY_TERM: "physics",
            CATEGORIES: {
                "Astrophysics - Cosmology and Nongalactic Astrophysics": "CO",
                "Astrophysics - Earth and Planetary Astrophysics": "EP",
                "Astrophysics - Astrophysics of Galaxies": "GA",
                "Astrophysics - High Energy Astrophysical Phenomena": "HE",
                "Astrophysics - Instrumentation and Methods for Astrophysics": "IM",
                "Astrophysics - Solar and Stellar Astrophysics": "SR",
            },
        },
        "cond-mat": {
            LABEL: "cond-mat",
            SEPARATOR: ".",
            QUERY_TERM: "physics",
            CATEGORIES: {
                "Condensed Matter - Disordered Systems and Neural Networks": "dis-nn",
                "Condensed Matter - Mesoscale and Nanoscale Physics": "mes-hall",
                "Condensed Matter - Materials Science": "mtrl-sci",
                "Condensed Matter - Other Condensed Matter": "other",
                "Condensed Matter - Quantum Gases": "quant-gas",
                "Condensed Matter - Soft Condensed Matter": "soft",
                "Condensed Matter - Statistical Mechanics": "stat-mech",
                "Condensed Matter - Strongly Correlated Electrons": "str-el",
                "Condensed Matter - Superconductivity": "supr-con",
            },
        },
        "gr-qc": {
            LABEL: "gr-qc",
            SEPARATOR: "",
            QUERY_TERM: "physics",
            CATEGORIES: {
                "General Relativity and Quantum Cosmology": "gr-qc",
            },
        },
        "hep": {
            LABEL: "hep",
            SEPARATOR: "-",
            QUERY_TERM: "physics",
            CATEGORIES: {
                "High Energy Physics - Experiment": "ex",
                "High Energy Physics - Lattice": "lat",
                "High Energy Physics - Phenomenology": "ph",
                "High Energy Physics - Theory": "th",
            },
        },
        "math-ph": {
            LABEL: "math-ph",
            SEPARATOR: "",
            QUERY_TERM: "physics",
            CATEGORIES: {
                "Mathematical Physics": "math-ph",
            },
        },
        "nlin": {
            LABEL: "nlin",
            SEPARATOR: ".",
            QUERY_TERM: "physics",
            CATEGORIES: {
                "Nonlinear Sciences - Adaptation and Self-Organizing Systems": "AO",
                "Nonlinear Sciences - Chaotic Dynamics": "CD",
                "Nonlinear Sciences - Cellular Automata and Lattice Gases": "CG",
                "Nonlinear Sciences - Pattern Formation and Solitons": "PS",
                "Nonlinear Sciences - Exactly Solvable and Integrable Systems": "SI",
            },
        },
        "nucl": {
            LABEL: "nucl",
            SEPARATOR: "-",
            QUERY_TERM: "physics",
            CATEGORIES: {
                "Nuclear Experiment": "ex",
                "Nuclear Theory": "th",
            },
        },
        "physics": {
            LABEL: "physics",
            SEPARATOR: ".",
            QUERY_TERM: "physics",
            CATEGORIES: {
                "Physics - Accelerator Physics": "acc-ph",
                "Physics - Atmospheric and Oceanic Physics": "ao-ph",
                "Physics - Applied Physics": "app-ph",
                "Physics - Atomic and Molecular Clusters": "atm-clus",
                "Physics - Atomic Physics": "atom-ph",
                "Physics - Biological Physics": "bio-ph",
                "Physics - Chemical Physics": "chem-ph",
                "Physics - Classical Physics": "class-ph",
                "Physics - Computational Physics": "comp-ph",
                "Physics - Data Analysis, Statistics and Probability": "data-an",
                "Physics - Physics Education": "ed-ph",
                "Physics - Fluid Dynamics": "flu-dyn",
                "Physics - General Physics": "gen-ph",
                "Physics - Geophysics": "geo-ph",
                "Physics - History of Physics": "hist-ph",
                "Physics - Instrumentation and Detectors": "ins-det",
                "Physics - Medical Physics": "med-ph",
                "Physics - Optics": "optics",
                "Physics - Plasma Physics": "plasm-ph",
                "Physics - Popular Physics": "pop-ph",
                "Physics - Physics and Society": "soc-ph",
                "Physics - Space Physics": "space-ph",
            },
        },
        "quant-ph": {
            LABEL: "quant-ph",
            SEPARATOR: "",
            QUERY_TERM: "physics",
            CATEGORIES: {
                "Quantum Physics": "quant-ph",
            },
        },
        "q-bio": {
            LABEL: "q-bio",
            SEPARATOR: ".",
            QUERY_TERM: "q-bio",
            CATEGORIES: {
                "Quantitative Biology - Biomolecules": "BM",
                "Quantitative Biology - Cell Behavior": "CB",
                "Quantitative Biology - Genomics": "GN",
                "Quantitative Biology - Molecular Networks": "MN",
                "Quantitative Biology - Neurons and Cognition": "NC",
                "Quantitative Biology - Other Quantitative Biology": "OT",
                "Quantitative Biology - Populations and Evolution": "PE",
                "Quantitative Biology - Quantitative Methods": "QM",
                "Quantitative Biology - Subcellular Processes": "SC",
                "Quantitative Biology - Tissues and Organs": "TO",
            },
        },
        "q-fin": {
            LABEL: "q-fin",
            SEPARATOR: ".",
            QUERY_TERM: "q-fin",
            CATEGORIES: {
                "Quantitative Finance - Computational Finance": "CP",
                "Quantitative Finance - Economics": "EC",
                "Quantitative Finance - General Finance": "GN",
                "Quantitative Finance - Mathematical Finance": "MF",
                "Quantitative Finance - Portfolio Management": "PM",
                "Quantitative Finance - Pricing of Securities": "PR",
                "Quantitative Finance - Risk Management": "RM",
                "Quantitative Finance - Statistical Finance": "ST",
                "Quantitative Finance - Trading and Market Microstructure": "TR",
            },
        },
        "bucket_name": os.environ.get("BUCKET_NAME"),
        "save_path": os.environ.get("SAVE_PATH"),
    }


GROUP_PREFIX_MAPPING = {
    "Computer Science": "cs",
    "Economics": "econ",
    "Electrical Engineering and Systems Science": "eess",
    "Mathematics": "math",
    "Astrophysics": "astro-ph",
    "Condensed Matter": "cond-mat",
    "General Relativity and Quantum Cosmology": "gr-qc",
    "High Energy Physics": "hep",
    "Mathematical Physics": "math-ph",
    "Nonlinear Sciences": "nlin",
    "Nuclear": "nucl",
    "Physics": "physics",
    "Quantum Physics": "quant-ph",
    "Quantitative Biology": "q-bio",
    "Quantitative Finance": "q-fin",
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
        bucket = event["Records"][0]["s3"]["bucket"]["name"]
        key = event["Records"][0]["s3"]["object"]["key"]
        logging.info(
            f"Processing arXiv daily summaries for bucket: {bucket}, key: {key}"
        )
    except KeyError as e:
        logging.error(f"Malformed event: {event}. Missing key: {e}")
        return {"statusCode": 400, "body": "Malformed event"}

    try:
        logging.info(f"Fetching S3 object for bucket: {bucket}, key: {key}")
        xml_data = fetch_s3_object(s3, bucket, key)
        logging.info(f"Fetched S3 object for bucket: {bucket}, key: {key}")
        extracted_data_chunk = parse_xml_data(xml_data)
        logging.info(f"Parsed XML data for bucket: {bucket}, key: {key}")
        upload_to_s3(s3, extracted_data_chunk, key)
    except Exception as e:
        logging.error(f"Error in Lambda handler: {str(e)}")
        return {"statusCode": 500, "body": "Failed to parse arXiv daily summaries"}

    logging.info("Successfully parsed arXiv daily summaries.")

    return {"statusCode": 200, "body": "Successfully parsed arXiv daily summaries"}


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
    sanitized_data = sanitize_object_data(
        response["Body"].read().decode("utf-8").strip()
    )
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

    Raises:
        Exception: If there is an error fetching the object.
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
    return "".join(ch for ch in raw_data if ch in {"\n", "\r"} or 32 <= ord(ch) <= 126)


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

    save_path = CONFIG.get("save_path")
    if save_path is None:
        logging.error("save_path is None in config.")
        return

    bucket_name = CONFIG.get("bucket_name")
    object_name = f"{save_path}/{key.replace('arxiv/', '')}-parsed.json"

    try:
        client.put_object(
            Body=json.dumps(data),
            Bucket=bucket_name,
            Key=object_name,
            ContentType="application/json",
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

    ns = {
        "oai": "http://www.openarchives.org/OAI/2.0/",
        "dc": "http://purl.org/dc/elements/1.1/",
    }
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
        extracted_data_chunk["records"].append(extract_record_data(record, ns))

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
    full_text_url = ""
    if abstract_url is not None:
        full_text_url = abstract_url.text.replace("/abs/", "/pdf/")
    authors = extract_authors(record, ns)
    logging.info(f"Extracted authors for record: {identifier.text}")
    groups, categories = extract_categories_and_groups(record, ns)
    logging.info(f"Extracted categories for record: {identifier.text}")
    groups = [group for group in groups if group]
    categories = [category for category in categories if category]
    primary_group = groups[0] if groups else ""
    primary_category = categories[0] if categories else ""
    abstract = record.find(".//dc:description", ns)
    title = record.find(".//dc:title", ns)
    date = record.find(".//dc:date", ns)
    logging.info(f"Extracted data for record: {identifier.text}")
    if any(el is None for el in [identifier, abstract_url, abstract, title, date]):
        logging.warning("Missing essential elements in record. Skipping.")
        return {}

    return {
        "identifier": identifier.text,
        "abstract_url": abstract_url.text,
        "full_text_url": full_text_url,
        "authors": authors,
        "primary_category": primary_category,
        "categories": categories,
        "abstract": abstract.text,
        "title": title.text,
        "date": date.text,
        "primary_group": primary_group,
        "groups": groups,
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
    return [
        {
            "last_name": name.text.split(", ", 1)[0],
            "first_name": name.text.split(", ", 1)[1]
            if len(name.text.split(", ", 1)) > 1
            else "",
        }
        for name in creators_elements
        if name.text
    ]


def extract_categories_and_groups(record, ns: dict) -> Tuple[List[str], List[str]]:
    """
    Extracts categories and groups from an arXiv research summary record.

    Args:
        record (ET.Element): The record element.
        ns (dict): A dict of namespaces.

    Returns:
        Tuple[List[str], List[str]]: A tuple of lists of categories and groups.
    """
    subjects_elements = record.findall(".//dc:subject", ns)
    matched_categories = []
    matched_groups = []

    for subject_element in subjects_elements:
        subject_text = subject_element.text
        if subject_text:
            for prefix, group in GROUP_PREFIX_MAPPING.items():
                if prefix in subject_text:
                    category_mapping = CONFIG.get(group, {}).get("categories", {})
                    matched_category = next(
                        (
                            abbr
                            for full, abbr in category_mapping.items()
                            if subject_text == full
                        ),
                        None,
                    )
                    if matched_category and matched_category not in matched_categories:
                        matched_categories.append(matched_category)
                    if group not in matched_groups:
                        matched_groups.append(group)
                    break
            else:
                logging.info(f"No match found for: {subject_text}")

    if not matched_categories:
        matched_categories.append("Unknown")

    if not matched_groups:
        matched_groups.append("Unknown")

    return matched_groups, matched_categories


if __name__ == "__main__":
    lambda_handler(
        {
            "Records": [
                {
                    "s3": {
                        "bucket": {"name": "techcraftingai-inbound-data"},
                        "object": {"key": "arxiv/cs-2023-10-30-3.xml"},
                    }
                }
            ]
        },
        None,
    )
