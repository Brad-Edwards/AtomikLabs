import datetime
from unittest.mock import patch, Mock

import pytest
import xml.etree.ElementTree as ET

import src.arxiv_fetch_daily_summaries as arxiv_fetch_daily_summaries


"""@pytest.mark.parametrize("event, expected_status_code, expected_body", [
    ({"base_url": "http://test.com", "bucket_name": "test_bucket", "from_date": (datetime.datetime.today() - datetime.timedelta(days=1)), "summary_set": "test_set"}, 200, "Successfully fetched arXiv daily summaries from 2022-01-01"),
    ({}, 200, "Successfully fetched arXiv daily summaries from None"),
])
def test_lambda_handler(event, expected_status_code, expected_body):
    with patch('src.arxiv_fetch_daily_summaries.fetch_arxiv_data') as mock_fetch_arxiv_data, patch('src.arxiv_fetch_daily_summaries.upload_to_s3') as mock_upload_to_s3:
        mock_fetch_arxiv_data.return_value = []
        context = None  # context is not used in the function, so it's safe to set it to None
        result = arxiv_fetch_daily_summaries.lambda_handler(event, context)
        assert result['statusCode'] == expected_status_code
        assert result['body'] == expected_body"""


def test_fetch_arxiv_data():
    with patch('src.arxiv_fetch_daily_summaries.fetch_data_from_endpoint') as mock_fetch_data_from_endpoint:
        mock_fetch_data_from_endpoint.side_effect = [("response1", "token1"), ("response2", None)]
        result = arxiv_fetch_daily_summaries.fetch_arxiv_data("http://test.com", "2022-01-01", "test_set")
        assert result == ["response1", "response2"]


def test_initialize_params():
    result = arxiv_fetch_daily_summaries.initialize_params("2022-01-01", "test_set")
    expected = {'verb': 'ListRecords', 'set': 'test_set', 'metadataPrefix': 'oai_dc', 'from': '2022-01-01'}
    assert result == expected


def test_fetch_data_from_endpoint_success():
    with patch('requests.get') as mock_get:
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.content = "<xml></xml>"
        mock_response.text = "<xml></xml>"
        mock_get.return_value = mock_response
        result, token = arxiv_fetch_daily_summaries.fetch_data_from_endpoint("http://test.com", {})
        assert result == "<xml></xml>"
        assert token is None


def test_find_resumption_token():
    xml = "<root><resumptionToken xmlns='http://www.openarchives.org/OAI/2.0/'>test_token</resumptionToken></root>"
    root = ET.fromstring(xml)
    result = arxiv_fetch_daily_summaries.find_resumption_token(root)
    assert result.text == "test_token"


def test_handle_http_error():
    with patch('time.sleep') as mock_sleep:
        mock_response = Mock()
        mock_response.status_code = 503
        mock_response.headers = {'Retry-After': '120'}
        arxiv_fetch_daily_summaries.handle_http_error(Exception("test"), mock_response, [30, 120])
        mock_sleep.assert_called_with(120)


def test_upload_to_s3():
    with patch('boto3.client') as mock_client:
        mock_s3 = Mock()
        mock_client.return_value = mock_s3
        arxiv_fetch_daily_summaries.upload_to_s3("test_bucket", "2022-01-01", "test_set", ["response1", "response2"])
        mock_s3.put_object.assert_called_with(Body='["response1", "response2"]', Bucket='test_bucket', Key='arxiv/test_set-2022-01-01.json')
