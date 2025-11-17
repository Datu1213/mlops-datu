# kafka_ml_processor.py
import json
import redis
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime
import pandas as pd # 用于时间计算
import logging
from typing import Dict, Any

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Kafka 配置
KAFKA_BROKER_URL = 'localhost:9094'
INPUT_TOPIC = 'user_events'
OUTPUT_TOPIC = 'ml_features' # 计算出的特征将发送到这个新Topic
CONSUMER_GROUP_ID = 'ml-feature-processor-group'
transctional_id = 'ml-feature-processor-1'

# Redis 配置
REDIS_HOST = 'localhost'
REDIS_PORT = 6379

# --- Kafka 客户端初始化 ---
try:
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BROKER_URL,
        group_id=CONSUMER_GROUP_ID,
        auto_offset_reset='latest', # 只处理新消息
        enable_auto_commit=False, # 我们将手动提交 offset
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER_URL,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all' # 保证消息被写入
    )
except Exception as e:
    logging.error(f"Failed to connect to Kafka: {e}")
    exit(1)

# --- Redis 客户端初始化 ---
try:
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
    redis_client.ping() # 测试连接
    logging.info("Connected to Redis successfully.")
except Exception as e:
    logging.error(f"Failed to connect to Redis: {e}")
    exit(1)

# --- 特征计算函数 ---

def calculate_time_since_last_event(user_id: str, current_timestamp_str: str, redis: redis.Redis) -> float | None:
    """从Redis获取上次事件时间戳，并计算时间差（秒）"""
    last_event_timestamp_str = redis.get(f"user:{user_id}:last_event_ts")
    if last_event_timestamp_str:
        try:
            current_dt = datetime.fromisoformat(current_timestamp_str.rstrip('Z'))
            last_dt = datetime.fromisoformat(last_event_timestamp_str.rstrip('Z'))
            time_diff = (current_dt - last_dt).total_seconds()
            return max(0, time_diff) # 避免负数
        except ValueError:
            logging.warning(f"Invalid timestamp format for user {user_id}")
            return None
    return None

def update_user_cache(user_id: str, event: Dict[str, Any], redis: redis.Redis):
    """更新Redis中的用户缓存信息"""
    user_key_prefix = f"user:{user_id}"
    event_timestamp = event['timestamp']

    # 使用Redis Pipeline批量执行命令，提高效率
    pipe = redis.pipeline()
    # 记录上次事件时间戳
    pipe.set(f"{user_key_prefix}:last_event_ts", event_timestamp)
    # 记录过去1小时的事件类型计数 (使用滑动窗口计数器的简化模拟)
    # 为简单起见，我们只增加总数，可以进一步按事件类型计数
    hourly_counter_key = f"{user_key_prefix}:events_last_hour"
    pipe.incr(hourly_counter_key)
    pipe.expire(hourly_counter_key, 3600) # 设置1小时过期

    pipe.execute()

# --- 主处理循环 ---

logging.info("Kafka Feature Processor started. Waiting for messages...")

try:
    for message in consumer:
        try:
            event = message.value
            user_id = event.get('user_id')
            timestamp = event.get('timestamp')

            if not user_id or not timestamp:
                logging.warning(f"Skipping message with missing user_id or timestamp: {event}")
                consumer.commit() # 即使跳过也要提交offset
                continue

            logging.info(f"Processing event for user: {user_id}")

            # 1. 从Redis获取/计算特征
            time_since_last = calculate_time_since_last_event(user_id, timestamp, redis_client)
            event_count_1h = redis_client.get(f"user:{user_id}:events_last_hour")
            event_count_1h = int(event_count_1h) if event_count_1h else 0

            # 2. 构造特征消息
            feature_message = {
                'user_id': user_id,
                'processing_timestamp': datetime.utcnow().isoformat() + 'Z',
                'original_event_timestamp': timestamp,
                'event_type_processed': event.get('event_type'),
                'time_since_last_event_sec': time_since_last,
                'event_count_last_hour': event_count_1h,
                # 可以添加更多基于事件内容或缓存计算的特征
            }

            # 3. 更新Redis缓存 (在发送到Kafka之前)
            update_user_cache(user_id, event, redis_client)

            # 4. 将特征发送到输出Topic
            producer.send(OUTPUT_TOPIC, value=feature_message)
            logging.info(f"Produced features for user {user_id}: {feature_message}")

            # 5. 最后，手动提交Offset，确保消息处理完成
            consumer.commit()

        except json.JSONDecodeError:
            logging.error(f"Failed to decode JSON message: {message.value}")
            consumer.commit() # 提交offset以跳过错误消息

        except Exception as e:
            logging.exception(f"Error processing message: {e}")
            # 发生意外错误时，不提交offset，以便稍后重试（取决于Kafka配置）

except KeyboardInterrupt:
    logging.info("Stopping feature processor...")
finally:
    consumer.close()
    producer.flush()
    producer.close()
    logging.info("Kafka clients closed.")