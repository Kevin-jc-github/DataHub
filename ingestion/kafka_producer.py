
from kafka import KafkaProducer, KafkaAdminClient
from kafka.errors import NoBrokersAvailable, KafkaTimeoutError, KafkaError  # 添加 KafkaError
import json
import random
import time
import uuid

# Kafka 配置
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092' 
TOPICS = ['user_clicks', 'order_events', 'payment_events']

# 初始化 Kafka Producer（带重试逻辑）
max_retries = 5
retry_count = 0

print("⏳ 尝试连接 Kafka 代理...")

admin_client = None
producer = None

while retry_count < max_retries:
    try:
        # 首先创建 AdminClient 测试连接
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            request_timeout_ms=5000
        )
        
        # 获取主题列表（验证连接）
        topics = admin_client.list_topics()
        print(f"✅ 成功连接到 Kafka 集群! 可用主题: {topics}")
        
        # 然后创建生产者
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None,
            acks='all',
            retries=3,
            batch_size=16384,
            linger_ms=10,
            buffer_memory=33554432,
            request_timeout_ms=30000
        )
        break
        
    except (NoBrokersAvailable, KafkaTimeoutError, KafkaError) as e:
        retry_count += 1
        print(f"⚠️ 连接失败，正在重试 {retry_count}/{max_retries}... 错误: {e}")
        time.sleep(5)
    finally:
        # 确保关闭 AdminClient
        if admin_client:
            try:
                admin_client.close()
            except:
                pass

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
    quantity = random.randint(1, 5)
    unit_price = round(random.uniform(10.0, 500.0), 2)
    total_amount = round(quantity * unit_price, 2)
    return {
        'event_id': str(uuid.uuid4()),
        'order_id': f"ord_{int(time.time())}_{random.randint(1000, 9999)}",
        'user_id': random.choice(USER_IDS),
        'product_id': random.choice(PRODUCT_IDS),
        'timestamp': int(time.time() * 1000),
        'order_status': random.choice(['created', 'confirmed', 'shipped', 'delivered', 'cancelled']),
        'quantity': quantity,
        'unit_price': unit_price,
        'total_amount': total_amount,
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

def main():
    print("🚀 高频Kafka Producer启动...")
    try:
        while True:
            # 发送20条点击事件
            for _ in range(20):
                click = generate_user_click()
                producer.send('user_clicks', key=click['user_id'], value=click)
                print(f"[click] 用户 {click['user_id']} 点击了 {click['product_id']}")

            # 每秒生成5条订单，其中80%支付
            for _ in range(5):
                order = generate_order_event()
                producer.send('order_events', key=order['user_id'], value=order)
                print(f"[order] 订单 {order['order_id']} 金额 {order['total_amount']} CNY")

                if random.random() < 0.8:
                    pay = generate_payment_event(order)
                    producer.send('payment_events', key=pay['user_id'], value=pay)
                    print(f"[payment] 支付 {pay['payment_id']} 状态 {pay['payment_status']}")

            # 每批发送后刷新
            producer.flush()
            print(f"📦 已发送一批消息，等待1秒...")
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n🛑 Producer 停止")
    finally:
        producer.close()

if __name__ == "__main__":
    main()

