import json
import time
import random
import uuid
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker
fake = Faker()

# Kafka 프로듀서 설정 (Docker로 띄운 로컬 카프카에 연결)
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'transactions'

# 테스트용 고정 유저 풀(Pool) 생성 (유저 고유의 패턴을 만들기 위함)
USER_POOL = [f"user_{i}" for i in range(1, 101)]
# 유저별 평소 결제 성향 (평균 금액) 정의
USER_PROFILES = {user: random.choice([20000, 50000, 150000]) for user in USER_POOL}

def generate_normal_transaction():
    """정상적인 일반 결제 데이터 생성"""
    user_id = random.choice(USER_POOL)
    base_amount = USER_PROFILES[user_id]
    
    # 평소 결제 금액의 +-20% 내에서 정상 결제 발생
    amount = int(base_amount * random.uniform(0.8, 1.2))
    
    tx = {
        "tx_id": str(uuid.uuid4()),
        "user_id": user_id,
        "timestamp": datetime.utcnow().isoformat(),
        "amount": amount,
        "merchant_category": random.choice(["shopping", "food", "mart", "travel"]),
        "device_id": f"dev_{user_id}",
        "ip_address": fake.ipv4()
    }
    return tx

def generate_fraud_velocity():
    """사기 패턴 1: 단시간 반복 결제 (동일 유저가 단시간에 폭발적 결제)"""
    user_id = random.choice(USER_POOL)
    tx_list = []
    
    # 동일 유저가 0.5초 간격으로 4번 결제하는 시나리오
    for _ in range(4):
        tx = {
            "tx_id": str(uuid.uuid4()),
            "user_id": user_id,
            "timestamp": datetime.utcnow().isoformat(),
            "amount": int(USER_PROFILES[user_id] * random.uniform(0.5, 1.0)),
            "merchant_category": "game_money",
            "device_id": f"dev_{user_id}",
            "ip_address": fake.ipv4()
        }
        tx_list.append(tx)
    return tx_list

def generate_fraud_spike():
    """사기 패턴 2: 금액 급증 (평소보다 수십 배 큰 금액 결제)"""
    user_id = random.choice(USER_POOL)
    
    tx = {
        "tx_id": str(uuid.uuid4()),
        "user_id": user_id,
        "timestamp": datetime.utcnow().isoformat(),
        "amount": random.choice([3000000, 5000000, 9000000]), # 갑작스러운 거액
        "merchant_category": "gift_card",
        "device_id": "dev_unknown_hacker", # 기기 변경 가정
        "ip_address": fake.ipv4()
    }
    return [tx]

if __name__ == "__main__":
    print("🚀 실시간 결제 데이터 생성기 작동 시작...")
    
    try:
        while True:
            # 90% 확률로 정상 데이터 생성, 10% 확률로 이상 거래(사기) 패턴 생성
            dice = random.random()
            
            if dice < 0.90:
                tx = generate_normal_transaction()
                producer.send(TOPIC_NAME, value=tx)
                print(f"[정상 결제 발송] User: {tx['user_id']}, Amount: {tx['amount']}")
                time.sleep(random.uniform(0.1, 0.5)) # 결제 주기 공백
            else:
                fraud_type = random.choice(["velocity", "spike"])
                if fraud_type == "velocity":
                    fraud_txs = generate_fraud_velocity()
                    for tx in fraud_txs:
                        producer.send(TOPIC_NAME, value=tx)
                        print(f"🔥 [이상 거래 - 반복결제 발생] User: {tx['user_id']}, Amount: {tx['amount']}")
                        time.sleep(0.1)
                elif fraud_type == "spike":
                    fraud_txs = generate_fraud_spike()
                    tx = fraud_txs[0]
                    producer.send(TOPIC_NAME, value=tx)
                    print(f"🚨 [이상 거래 - 금액급증 발생] User: {tx['user_id']}, Amount: {tx['amount']}")
                    time.sleep(random.uniform(0.1, 0.5))
                    
    except KeyboardInterrupt:
        print("\n🛑 데이터 생성기가 중지되었습니다.")
    finally:
        producer.flush()
