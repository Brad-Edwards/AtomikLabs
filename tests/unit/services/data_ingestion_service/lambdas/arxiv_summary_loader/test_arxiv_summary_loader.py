import json
from unittest.mock import patch, MagicMock
import pytest

from data_ingestion_service.lambdas.arxiv_summary_loader.src import arxiv_summary_loader

def test_lambda_handler():
    s3_event = {
        'Records': [
            {
                's3': {
                    'bucket': {
                        'name': 'test-bucket'
                    },
                    'object': {
                        'key': 'test-key'
                    }
                }
            }
        ]
    }

    with patch('your_lambda_file.boto3') as mock_boto3, \
            patch('your_lambda_file.insert_record') as mock_insert_record:
        mock_s3 = MagicMock()
        mock_boto3.client.return_value = mock_s3

        mock_s3.get_object.return_value = {
            'Body': json.dumps({'records': [{'identifier': 'test-identifier'}]})
        }

        arxiv_summary_loader.lambda_handler(s3_event, None)

        mock_insert_record.assert_called_once_with({'identifier': 'test-identifier'})


def test_parse_record():
    record = {
        'identifier': 'test-identifier',
        'abstract_url': 'test-abstract-url',
        'authors': [
            {
                'last_name': 'test-last-name-1',
                'first_name': 'test-first-name-1'
            },
            {
                'last_name': 'test-last-name-2',
                'first_name': 'test-first-name-2'
            },
        ],
        'primary_category': 'test-primary-category',
        'categories': ['test-category-1', 'test-category-2'],
        'abstract': 'test-abstract',
        'title': 'test-title',
        'date': '2023-10-30',
        'group': 'test-group'
    }

    result = arxiv_summary_loader.parse_record(record)

    assert result['unique_identifier'] == 'test-identifier'
    assert result['abstract_url'] == 'test-abstract-url'
    assert result['title'] == 'test-title'
    assert result['primary_category'] == 'test-primary-category'
    assert result['summary'] == 'test-abstract'
    assert result['date'] == '2023-10-30'
    assert result['group_name'] == 'test-group'
    assert result['categories'] == ['test-category-1', 'test-category-2']
    assert len(result['authors']) == 2
    assert result['authors'][0]['last_name'] == 'test-last-name-1'
    assert result['authors'][0]['first_name'] == 'test-first-name-1'


def test_insert_record():
    record = {
        'unique_identifier': 'test-identifier',
        'abstract_url': 'test-abstract-url',
        'title': 'test-title',
        'primary_category': 'test-primary-category',
        'summary': 'test-abstract',
        'date': '2023-10-30',
        'group_name': 'test-group',
        'categories': ['test-category-1', 'test-category-2'],
        'authors': [
            {
                'last_name': 'test-last-name-1',
                'first_name': 'test-first-name-1'
            },
            {
                'last_name': 'test-last-name-2',
                'first_name': 'test-first-name-2'
            },
        ]
    }

    with patch('your_lambda_file.psycopg2') as mock_psycopg2:
        mock_connection = MagicMock()
        mock_cursor = MagicMock()

        mock_psycopg2.connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor

        insert_record(record)

        assert mock_cursor.execute.call_count == 6  # 1 research + 2 authors + 1 research_author junction table for each author
        mock_connection.commit.assert_called_once()
