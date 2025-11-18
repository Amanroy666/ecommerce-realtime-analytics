"""Kafka Consumer for E-Commerce Transactions"""
from kafka import KafkaConsumer
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TransactionConsumer:
    def __init__(self, bootstrap_servers, topic):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
    
    def process_transaction(self, transaction):
        logger.info(f"Processing: {transaction['transaction_id']}")
        
    def consume_messages(self):
        for message in self.consumer:
            self.process_transaction(message.value)
