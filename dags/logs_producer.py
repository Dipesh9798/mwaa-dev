import json
import random
from datetime import timedelta, datetime

import boto3
import botocore
from airflow import DAG
from airflow.operators.python import PythonOperator
from botocore.config import Config
from confluent_kafka import Producer
from faker import Faker
import logging

# from dags.utils import get_secret

fake = Faker()
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

def create_kafka_producer(config):
    return Producer(config)

def generate_log():
    """Generate synthetic log"""
    methods = ['GET', 'POST', 'PUT', 'DELETE']
    endpoints = ['/api/users', '/home', '/about', '/contact', '/services']
    statuses = [200,301,302,400,404,500]

    user_agents = [
        'Mozilla/5.0 (iPhone: CPU iPhone OS 14_6 like Mac OS X)',
        'Mozilla/5.0 (X11; Linux x86_64)',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
        'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    ]

    referrers = ['https://example.com', 'https://goggle.com', '-', 'https://bing.com', 'https://yahoo.com']

    ip = fake.ipv4()
    timestamp = datetime.now().strftime("%b %d %Y, %H:%M:%S")
    method = random.choice(methods)
    endpoint = random.choice(endpoints)
    status = random.choice(statuses)
    size = random.randint(1000,15000)
    referrer = random.choice(referrers)
    user_agent = random.choice(user_agents)

    log_entry = (
        f'{ip} - - [{timestamp}] "{method} {endpoint} HTTP/1.1" {status} {size} "{referrer}" {user_agent}'
    )

    return log_entry

def delivery_report(err, msg):
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def produce_logs(**context):
    """Produce log entries into Kafka"""
    secrets = get_secret('MWAA_Secrets_V2')
    kafka_config = {
        'bootstrap.servers': secrets['KAFKA_BOOTSTRAP_SERVER'],
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': secrets['KAFKA_SASL_USERNAME'],
        'sasl.password': secrets['KAFKA_SASL_PASSWORD'],
        'session.timeout.ms': 50000
    }

    producer = create_kafka_producer(kafka_config)
    topic = 'billion_website_logs'

    for i in range(100):
        log = generate_log()
        try:
            producer.produce(topic, log.encode('utf-8'), on_delivery= delivery_report)
            producer.flush()
        except Exception as e:
            logger.error(f'Error producing log: {e}')
            raise

    logger.info(f'Produced 100 logs to topic {topic}')


default_args = {
    'owner': 'Data Mastery Lab',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

dag = DAG(
    'log_generation_pipeline',
    default_args = default_args,
    description= 'Generate and produce synthetic logs',
    schedule_interval= '*/5 * * * *',
    start_date= datetime(2025, 2, 7),
    catchup= False,
    tags= ['logs', 'kafka', 'dev']
)

produce_logs_task = PythonOperator(
    task_id= 'generate_and_produce_logs',
    python_callable= produce_logs,
    dag= dag
)

# produce_logs()