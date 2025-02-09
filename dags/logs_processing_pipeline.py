import json
import random
import re
from datetime import timedelta, datetime

import boto3
import botocore
from airflow import DAG
from airflow.operators.python import PythonOperator
from botocore.config import Config
from confluent_kafka import Consumer, KafkaException, KafkaError
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from faker import Faker
import logging

# from dags.utils import get_secret

logger = logging.getLogger(__name__)

def get_secret(secret_name, region_name = 'us-east-1'):
    """Retrieving secrets from aws secrets manager"""
    session = boto3.Session()
    client = session.client(service_name= 'secretsmanager', region_name= region_name)
    try:
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])
    except Exception as e:
        logger.error("Secret retrieval error")
        raise

def parse_log_entry(log_entry):
    """Parse log entry"""
    log_pattern = r'(?P<ip>[\d\.]+) - - \[(?P<timestamp>.*)\] "(?P<method>\w+) (?P<endpoint>[\w/]+) (?P<protocol>[\w/\.]+)'
    match = re.match(log_pattern, log_entry)
    if not match:
        logger.warning(f'Invalid log format: {log_entry}')
        return None
    data = match.groupdict()

    try:
        parsed_timestamp = datetime.strptime(data['timestamp'], '%b %d %Y, %H:%M:%S')
        data['timestamp'] = parsed_timestamp.isoformat()
    except ValueError:
        logger.warning(f'Timestamp parsing error: {data["timestamp"]}')
        return None
    return data

def consume_and_index_logs(**Context):
    """Consume logs from Kafka and index in Elasticsearch"""
    secrets = get_secret('MWAA_Secrets_V2')
    consumer_config = {
        'bootstrap.servers': secrets['KAFKA_BOOTSTRAP_SERVER'],
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': secrets['KAFKA_SASL_USERNAME'],
        'sasl.password': secrets['KAFKA_SASL_PASSWORD'],
        'group.id': 'mwaa_log_indexer',
        'auto.offset.reset': 'latest'
    }

    es_config = {
        'hosts': [secrets['ELASTICSEARCH_URL']],
        'api_key': secrets['ELASTICSEARCH_API_KEY']
    }

    consumer = Consumer(consumer_config)
    es_client = Elasticsearch(**es_config)
    topic = 'billion_website_logs'
    consumer.subscribe([topic])
    index_name = 'billion_website_logs'

    try:
        if not es_client.indices.exists(index=index_name):
            es_client.indices.create(index_name)
            logger.info(f'Created index: {index_name}')
    except Exception as e:
        logger.error(f'Failed creating index: {index_name} {e}')
        raise

    logs = []
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    break
                raise KafkaException(msg.error())

            log_entry = msg.value().decode('utf-8')
            parsed_log = parse_log_entry(log_entry)

            if parsed_log is None:
                logs.append(parsed_log)

            #index when 15000 logs are parsed
            if len(logs) >= 15000:
                print('Indexing logs')
                actions = [
                    {
                        '_op_type': 'create',
                        '_index': index_name,
                        '_source': log
                    }
                    for log in logs
                ]
                success, failed = bulk(es_client, actions, refresh=True)
                logger.info(f'Indexed {success} logs, {len(failed)} failed')
            logs = []
    except Exception as e:
        logger.error(f'Failed to index log: {e}')

    try:
        #index remaining logs
        if logs:
            actions = [
                {
                    '_op_type': 'create',
                    '_index': index_name,
                    '_source': log
                }
                for log in logs
            ]
            bulk(es_client, actions, refresh=True)
            logger.info(f'Indexed logs')
    except Exception as e:
        logger.error(f'Log processing error: {e}')
    finally:
        consumer.close()
        es_client.close()


default_args = {
    'owner': 'Data Mastery Lab',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

dag = DAG(
    'log_consumer_pipeline',
    default_args = default_args,
    description= 'Consume and Index synthetic logs',
    schedule= '*/5 * * * *',
    start_date= datetime(2025, 2, 7),
    catchup= False,
    tags= ['logs', 'kafka', 'dev']
)

consume_logs_task = PythonOperator(
    task_id= 'generate_and_consume_logs',
    python_callable= consume_and_index_logs,
    dag= dag
)

# consume_and_index_logs()