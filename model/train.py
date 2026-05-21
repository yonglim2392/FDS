import psycopg2
import pandas as pd
import joblib
from sklearn.ensemble import RandomForestClassifier
from datetime import datetime
import uuid
import random

def bootstrap_historical_data(conn, cursor):
    """[Data Engineering Layer] DB에 데이터가 부족할 경우 학습 가능한 최소 대용량 데이터 적재"""
    print("⚠️ DB 내 데이터가 부족하여 모델 학습용 초기 합성 데이터를 구축합니다 (약 5,000건)...")
    
    user_pool = [f"user_{i}" for i in range(1, 200)]
    categories = ["shopping", "food", "mart", "travel", "game_money", "gift_card"]
    
    bulk_data = []
    for _ in range(5000):
        user_id = random.choice(user_pool)
        is_fraud = random.random() < 0.03 # 3% 사기 확률
        
        if is_fraud:
            amount = random.choice([3000000, 5000000, 8000000])
            category = random.choice(["game_money", "gift_card"])
            device = "dev_unknown_hacker"
            tx_count = random.randint(3, 15)
            total_amount = amount + random.randint(500000, 2000000)
            decision = "BLOCKED"
        else:
            amount = random.randint(10000, 150000)
            category = random.choice(["shopping", "food", "mart", "travel"])
            device = f"dev_{user_id}"
            tx_count = random.randint(1, 2)
            total_amount = amount
            decision = "APPROVED"
            
        bulk_data.append((
            str(uuid.uuid4()), user_id, datetime.utcnow(), amount, category, 
            device, "127.0.0.1", 100 if is_fraud else 0, decision, tx_count, total_amount
        ))
        
    query = """
        INSERT INTO transaction_logs 
        (tx_id, user_id, tx_timestamp, amount, merchant_category, device_id, ip_address, fraud_score, decision, tx_count_10m, total_amount_10m)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.executemany(query, bulk_data)
    conn.commit()
    print("✅ 초기 데이터 셋 동기화 완료.")

def train_fds_model():
    conn = psycopg2.connect(host="localhost", database="fds_db", user="fds_user", password="fds_password", port="5432")
    cursor = conn.cursor()
    
    # 1. 데이터 확인 및 콜드 스타트 방지 처리
    cursor.execute("SELECT COUNT(*) FROM transaction_logs;")
    count = cursor.fetchone()[0]
    if count < 100:
        bootstrap_historical_data(conn, cursor)
        
    # 2. 파이프라인 학습용 피처 추출
    query = "SELECT amount, merchant_category, device_id, tx_count_10m, total_amount_10m, decision FROM transaction_logs;"
    df = pd.read_sql(query, conn)
    
    # 3. 데이터 전처리 및 피처 엔지니어링 (파이프라인 서빙 안정성을 위해 파생 피처 단순화)
    df['is_high_risk_cat'] = df['merchant_category'].apply(lambda x: 1 if x in ["game_money", "gift_card"] else 0)
    df['is_hacker_dev'] = df['device_id'].apply(lambda x: 1 if x == "dev_unknown_hacker" else 0)
    df['target'] = df['decision'].apply(lambda x: 1 if x == "BLOCKED" else 0)
    
    X = df[['amount', 'tx_count_10m', 'total_amount_10m', 'is_high_risk_cat', 'is_hacker_dev']]
    y = df['target']
    
    # 4. 머신러닝 모델 학습 (Random Forest)
    print("🧠 FDS 머신러닝 예측 모델 최적화 및 학습 중...")
    model = RandomForestClassifier(n_estimators=100, max_depth=6, random_state=42)
    model.fit(X, y)
    
    # 5. 모델 바이너리 파일 내보내기
    joblib.dump(model, 'model/fds_model.pkl')
    print("💾 모델 저장이 완료되었습니다: model/fds_model.pkl")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    train_fds_model()
