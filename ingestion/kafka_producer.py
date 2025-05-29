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

def generate_order_event():
    """生成订单事件数据"""
    quantity =  random.randint(1, 5),
    unit_price = round(random.uniform(10.0, 500.0), 2),
    return {
        'event_id': str(uuid.uuid4()),
        'order_id': f"ord_{int(time.time())}_{random.randint(1000, 9999)}",
        'user_id': random.choice(USER_IDS),
        'product_id': random.choice(PRODUCT_IDS),
        'timestamp': int(time.time() * 1000),
        'order_status': random.choice(['created', 'confirmed', 'shipped', 'delivered', 'cancelled']),
        'quantity': quantity,
        'unit_price': unit_price,
        'total_amount': round(quantity * unit_price),
        'currency': 'CNY',
        'shipping_address': {
            'city': random.choice(['北京', '上海', '广州', '深圳', '杭州', '南京']),
            'district': random.choice(['朝阳区', '海淀区', '浦东新区', '天河区', '南山区'])
        },
        'payment_method': random.choice(['alipay', 'wechat_pay', 'credit_card', 'bank_transfer'])
    }

def generate_payment_event(order):
    """生成支付事件数据"""
    return {
        'event_id': str(uuid.uuid4()),
        'payment_id': f"pay_{int(time.time())}_{random.randint(1000, 9999)}",
        'order_id': order['order_id'],
        'user_id': order['user_id'],
        'timestamp': int(time.time() * 1000),
        'payment_status': random.choice(['pending', 'processing', 'success', 'failed', 'refunded']),
        'payment_method': order['payment_method'],
        'amount': order['total_amount'],
        'currency': order['currency'],
        'transaction_id': f"txn_{int(time.time())}_{random.randint(10000, 99999)}",
        'gateway': random.choice(['alipay_gateway', 'wechat_gateway', 'unionpay_gateway']),
        'fee': round(order['total_amount'] * 0.006, 2),  # 手续费约0.6%
        'risk_score': round(random.uniform(0.1, 0.9), 2),
        'ip_address': f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}",
        'device_info': {
            'device_type': random.choice(['mobile', 'desktop', 'tablet']),
            'os': random.choice(['iOS', 'Android', 'Windows', 'macOS']),
            'browser': random.choice(['Chrome', 'Safari', 'Firefox', 'Edge'])
        }
    }



