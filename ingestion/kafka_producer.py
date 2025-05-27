from kafka import KafkaProducer
import json
import random
import time
import uuid

# Kafka 配置
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
TOPICS = ['user_clicks', 'order_events', 'payment_events']

# 初始化 Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8') if k else None,
    acks='all',  # 等待所有副本确认
    retries=3,   # 重试次数
    batch_size=16384,  # 批处理大小
    linger_ms=10,      # 等待时间
    buffer_memory=33554432  # 缓冲区大小
)

USER_IDS = [f"u{i}" for i in range(1000, 1100)]
PRODUCT_IDS = [f"p{i}" for i in range(2000, 2100)]

def generate_user_click():
    """生成用户点击事件数据"""
    return {
        'event_id': str(uuid.uuid4()),
        'user_id': random.choice(USER_IDS),
        'product_id': random.choice(PRODUCT_IDS),
        'timestamp': int(time.time() * 1000),
        'page_type': random.choice(['home', 'category', 'product', 'search']),
        'session_id': str(uuid.uuid4())[:8],
        'ip_address': f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}",
        'user_agent': random.choice(['Chrome', 'Firefox', 'Safari', 'Edge'])
    }




