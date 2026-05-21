# model/train.py 고도화 수정본
import psycopg2
import pandas as pd
import joblib
from sklearn.ensemble import RandomForestClassifier
from datetime import datetime, timedelta
import uuid
import random

def bootstrap_historical_data(conn, cursor):
    """[Data Engineering Layer] 람다 아키텍처 스키마에 맞춘 초기 대량 합성 데이터 적재"""
    print("⚠️ DB 내 데이터가 부족하여 람다 피처 스키마가 반영된 초기 합성 데이터를 구축합니다 (약 5,000건)...")
    
    user_pool = [f"user_{i}" for i in range(1, 200)]
    categories = ["shopping", "food", "mart", "travel", "game_money", "gift_card"]
    
    bulk_data = []
    base_time = datetime.utcnow() - timedelta(days=30) # 30일 전부터 데이터가 쌓인 것으로 모사
    
    for i in range(5000):
        user_id = random.choice(user_pool)
        is_fraud = random.random() < 0.03
        tx_time = base_time + timedelta(minutes=i * 8) # 시계열 분포 분산
        
        if is_fraud:
            amount = random.choice([3000000, 5000000, 8000000])
            category = random.choice(["game_money", "gift_card"])
            device = "dev_unknown_hacker"
            tx_count = random.randint(3, 15)
            total_amount = amount + random.randint(500000, 2000000)
            decision = "BLOCKED"
            avg_amount_30d = random.randint(20000, 100000) # 평소엔 적게 쓰던 유저로 모사
        else:
            amount = random.randint(10000, 150000)
            category = random.choice(["shopping", "food", "mart", "travel"])
            device = f"dev_{user_id}"
            tx_count = random.randint(1, 2)
            total_amount = amount
            decision = "APPROVED"
            avg_amount_30d = int(amount * random.uniform(0.8, 1.2)) # 평소 쓰던 양과 비슷하게 모사
            
        bulk_data.append((
            str(uuid.uuid4()), user_id, tx_time, amount, category, 
            device, "127.0.0.1", 100 if is_fraud else 0, decision, tx_count, total_amount, avg_amount_30d
        ))
        
    query = """
        INSERT INTO transaction_logs 
        (tx_id, user_id, tx_timestamp, amount, merchant_category, device_id, ip_address, fraud_score, decision, tx_count_10m, total_amount_10m, avg_amount_30d)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    # 주의: 데이터베이스에 avg_amount_30d 컬럼이 없다면 먼저 ALTER TABLE로 추가해야 함.
    cursor.executemany(query, bulk_data)
    conn.commit()
    print("✅ 초기 데이터 셋 동기화 완료.")

def train_fds_model():
    conn = psycopg2.connect(host="localhost", database="fds_db", user="fds_user", password="fds_password", port="5432")
    cursor = conn.cursor()
    
    # 데이터베이스 스키마 확장 여부 선제 확인 및 적용
    try:
        cursor.execute("ALTER TABLE transaction_logs ADD COLUMN avg_amount_30d INT DEFAULT 0;")
        conn.commit()
    except Exception:
        conn.rollback()

    cursor.execute("SELECT COUNT(*) FROM transaction_logs;")
    count = cursor.fetchone()[0]
    if count < 100:
        bootstrap_historical_data(conn, cursor)
        
    # [수정] 람다 아키텍처의 정적 배치 피처인 avg_amount_30d를 학습 대상에 명시적 포함
    query = "SELECT amount, merchant_category, device_id, tx_count_10m, total_amount_10m, avg_amount_30d, decision FROM transaction_logs;"
    df = pd.read_sql(query, conn)
    
    df['is_high_risk_cat'] = df['merchant_category'].apply(lambda x: 1 if x in ["game_money", "gift_card"] else 0)
    df['is_hacker_dev'] = df['device_id'].apply(lambda x: 1 if x == "dev_unknown_hacker" else 0)
    
    # 파생 피처 엔지니어링: 30일 평균 대비 현재 결제 비율을 모델에게 직접 학습시킴
    df['spending_ratio_vs_30d'] = df.apply(
        lambda r: float(r['amount'] / r['avg_amount_30d']) if r['avg_amount_30d'] > 0 else 1.0, axis=1
    )
    df['target'] = df['decision'].apply(lambda x: 1 if x == "BLOCKED" else 0)
    
    # 모델 입력 피처 스키마 동기화 완료
    X = df[['amount', 'tx_count_10m', 'total_amount_10m', 'avg_amount_30d', 'spending_ratio_vs_30d', 'is_high_risk_cat', 'is_hacker_dev']]
    y = df['target']
    
    print("🧠 [Lambda Machine Learning] 통합 피처 기반 FDS 모델 최적화 및 학습 중...")
    model = RandomForestClassifier(n_estimators=100, max_depth=6, random_state=42)
    model.fit(X, y)
    
    joblib.dump(model, 'model/fds_model.pkl')
    print("💾 람다 동기화 모델 저장 완료: model/fds_model.pkl")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    train_fds_model()
