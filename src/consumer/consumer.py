import json
from kafka import KafkaConsumer
from datetime import datetime
# 创建一个Kafka消费者实例
# bootstrap_servers 指向我们在docker-compose中暴露的Kafka端口
consumer = KafkaConsumer(
    'user_events',  # 订阅的Topic名称
    bootstrap_servers='localhost:9094',  # 注意端口是9094！
    auto_offset_reset='earliest',  # 从最早的消息开始读取
    enable_auto_commit=True, # 自动提交 offset
    group_id='my-consumer-group',  # 消费者组ID
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))  # 自动将JSON反序列化为字典
)
print("Kafka Consumer started. Listening for messages...")
try:
    for message in consumer:
        event = message.value
        print(f"Received event: {event}")
except KeyboardInterrupt:
    print("Stopping consumer...")
finally:
    consumer.close()
    print("Consumer closed.")