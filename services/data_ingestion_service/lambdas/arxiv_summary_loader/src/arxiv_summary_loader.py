import json
import boto3
import psycopg2
from psycopg2 import sql
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    try:
        bucket, key = get_s3_event_details(event)
        content = get_s3_file_content(bucket, key)
        data = json.loads(content)
        process_records(data['records'])
    except Exception as e:
        logger.error(f"Error processing lambda function: {str(e)}")
        raise


def get_s3_event_details(event):
    try:
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        return bucket, key
    except Exception as e:
        logger.error(f"Error getting S3 event details: {str(e)}")
        raise


def get_s3_file_content(bucket, key):
    try:
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        return content
    except Exception as e:
        logger.error(f"Error getting S3 file content from bucket {bucket}, key {key}: {str(e)}")
        raise


def process_records(records):
    try:
        conn = get_postgresql_connection()
        cursor = conn.cursor()
        for record in records:
            process_record(record, cursor)
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error processing records: {str(e)}")
        raise


def get_postgresql_connection():
    try:
        return psycopg2.connect(
            host=os.environ.get("DATABASE_HOST"),
            port=os.environ.get("DATABASE_PORT"),
            user=os.environ.get("DATABASE_USER"),
            password=os.environ.get("DATABASE_PASSWORD"),
            dbname=os.environ.get("DATABASE_NAME"),
            sslmode=os.environ.get("DATABASE_SSL_MODE"),
        )
    except Exception as e:
        logger.error(f"Error getting PostgreSQL connection: {str(e)}")
        raise


def process_record(record, cursor):
    try:
        research_id = insert_research(record, cursor)
        for author in record.get('authors', []):
            author_id = insert_author(author, cursor)
            insert_research_author(research_id, author_id, cursor)
    except Exception as e:
        logger.error(f"Error processing record: {str(e)}")
        raise


def insert_research(record, cursor):
    try:
        if 'title' not in record or 'primary_category' not in record or 'abstract' not in record or 'date' not in record or 'identifier' not in record or 'abstract_url' not in record or 'categories' not in record or 'group' not in record:
            raise ValueError("Missing required fields in input data")
        full_text_url = record['abstract_url'].replace('/abs/', '/pdf/')
        categories = record['categories'].replace('"{', '{').replace('}"', '}').replace('""', '"')
        if categories:
            categories = json.loads(categories)
        else:
            categories = []
        cursor.execute(sql.SQL("""
            INSERT INTO research (
                title, primary_category, summary, date, unique_identifier, 
                abstract_url, full_text_url, stored_pdf_url, 
                stored_full_text_url, categories
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (unique_identifier)
            DO UPDATE SET
                title = excluded.title,
                primary_category = excluded.primary_category,
                summary = excluded.summary,
                date = excluded.date,
                abstract_url = excluded.abstract_url,
                full_text_url = excluded.full_text_url,
                stored_pdf_url = excluded.stored_pdf_url,
                stored_full_text_url = excluded.stored_full_text_url,
                categories = excluded.categories
            RETURNING research_id
        """), (
            record['title'],
            record['primary_category'],
            record['abstract'],
            record['date'],
            record['identifier'],
            record['abstract_url'],
            full_text_url,  # full_text_url
            None,  # stored_pdf_url
            None,  # stored_full_text_url
            categories  # categories
        ))
        research_id = cursor.fetchone()[0]

        for group_name in record['group']:
            cursor.execute(sql.SQL("""
                INSERT INTO research_groups (group_name)
                VALUES (%s)
                ON CONFLICT (group_name)
                DO NOTHING
                RETURNING group_id
            """), (group_name,))
            group_id = cursor.fetchone()[0]

            is_primary = group_name == record['primary_group']
            cursor.execute(sql.SQL("""
                INSERT INTO research_group (research_id, group_id, is_primary)
                VALUES (%s, %s, %s)
                ON CONFLICT (research_id, group_id)
                DO NOTHING
            """), (research_id, group_id, is_primary))

        return research_id
    except Exception as e:
        logger.error(f"Error inserting research: {str(e)}")
        raise


def insert_author(author, cursor):
    try:
        cursor.execute(sql.SQL("""
            INSERT INTO research_authors (first_name, last_name)
            VALUES (%s, %s)
            ON CONFLICT (first_name, last_name)
            DO NOTHING;
        """), (author['first_name'], author['last_name']))
        cursor.execute(sql.SQL("""
            SELECT author_id
            FROM research_authors
            WHERE first_name = %s AND last_name = %s;
        """), (author['first_name'], author['last_name']))
        return cursor.fetchone()[0]
    except Exception as e:
        logger.error(f"Error inserting author: {str(e)}")
        raise


def insert_research_author(research_id, author_id, cursor):
    try:
        cursor.execute(sql.SQL("""
            INSERT INTO research_author (research_id, author_id)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
        """), (research_id, author_id))
    except Exception as e:
        logger.error(f"Error inserting research_author: {str(e)}")
        raise
