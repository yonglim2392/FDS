import json
import redis
import psycopg2
import joblib
import pandas as pd
from kafka import KafkaConsumer

# 인프라 자원 초기화
redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
model = joblib.load('model/fds_model.pkl')

db_conn = psycopg2.connect(host="localhost", database="fds_db", user="fds_user", password="fds_password", port="5432")
db_cursor = db_conn.cursor()

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='fds-inference-group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

def predict_lambda_fds(tx_data, streaming_features, batch_features):
    """
    [Lambda Architecture Inference Layer]
    실시간 Raw + 스트리밍(10분) + 배치(30일) 피처를 다차원 결합하여 추론 수행.
    """
    current_amount = tx_data['amount']
    
    # 1. 스트리밍 피처 추출
    tx_count_10m = int(streaming_features.get('tx_count_last_10m', 0))
    total_amount_10m = int(streaming_features.get('total_amount_last_10m', 0))
    
    # 2. 배치 피처 추출 (과거 이력이 아예 없는 신규 유저는 현재 금액을 기준으로 기본 셋팅)
    avg_amount_30d = int(batch_features.get('avg_amount_30d', current_amount))
    
    # 3. 파생 피처 엔지니어링: 평소 30일 평균 결제액 대비 현재 요청 금액의 배율 연산
    # 만약 평소에 1만 원 쓰던 사람이 50만 원을 쓰면 비율이 50배로 치솟음 -> 강력한 사기 징후
    spending_ratio_vs_30d = float(current_amount / avg_amount_30d) if avg_amount_30d > 0 else 1.0
    
    is_high_risk_cat = 1 if tx_data.get('merchant_category') in ["game_money", "gift_card"] else 0
    is_hacker_dev = 1 if tx_data.get('device_id') == "dev_unknown_hacker" else 0
    
    # 머신러닝 모델 입력값 생성 (학습된 의사결정 나무 스키마에 전달)
    input_df = pd.DataFrame([{
        'amount': current_amount,
        'tx_count_10m': tx_count_10m,
        'total_amount_10m': total_amount_10m,
        'is_high_risk_cat': is_high_risk_cat,
        'is_hacker_dev': is_hacker_dev
        # 참고: 실제 프로덕션 단계선 fds_model.pkl 학습 시 spending_ratio_vs_30d를 추가해 재학습 시킵니다.
    }])
    
    fraud_probability = model.predict_proba(input_df)[0][1]
    fraud_score = int(fraud_probability * 100)
    
    # [하이브리드 룰 보완] 모델 점수가 낮더라도 평소 쓰던 금액보다 10배 이상 과소비하면 강제 차단선 가중치 부여
    if spending_ratio_vs_30d >= 10.0 and current_amount >= 100000:
        fraud_score = max(fraud_score, 85)
        
    return fraud_score, avg_amount_30d

def save_to_postgres(tx_data, fraud_score, decision, streaming_features):
    tx_count_10m = int(streaming_features.get('tx_count_last_10m', 0))
    total_amount_10m = int(streaming_features.get('total_amount_last_10m', 0))
    
    query = """
        INSERT INTO transaction_logs 
        (tx_id, user_id, tx_timestamp, amount, merchant_category, device_id, ip_address, fraud_score, decision, tx_count_10m, total_amount_10m)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    try:
        db_cursor.execute(query, (
            tx_data['tx_id'], tx_data['user_id'], tx_data['timestamp'], tx_data['amount'],
            tx_data['merchant_category'], tx_data['device_id'], tx_data['ip_address'],
            fraud_score, decision, tx_count_10m, total_amount_10m
        ))
        db_conn.commit()
    except Exception as e:
        print(f"❌ DB 적재 에러: {e}")
        db_conn.rollback()

if __name__ == "__main__":
    print("🛡️ [완전판 람다 FDS] 실시간 스트리밍+배치 융합형 추론 엔진 가동...")
    print("-" * 80)
    
    try:
        for message in consumer:
            tx_data = message.value
            user_id = tx_data['user_id']
            
            # [Feature Store 온라인 조인 1] 실시간 스트리밍 피처 읽기
            stream_key = f"fds:user:{user_id}"
            streaming_features = redis_client.hgetall(stream_key)
            
            # [Feature Store 온라인 조인 2] 장기 배치 피처 읽기
            batch_key = f"fds:user:{user_id}:batch"
            batch_features = redis_client.hgetall(batch_key)
            
            # 다차원 피처 결합 기반 스코어링
            fraud_score, avg_30d = predict_lambda_fds(tx_data, streaming_features, batch_features)
            decision = "BLOCKED" if fraud_score >= 70 else "APPROVED"
            
            save_to_postgres(tx_data, fraud_score, decision, streaming_features)
            
            if decision == "BLOCKED":
                print(f"🚨 [람다 차단 조치] 유저: {user_id} | 금액: {tx_data['amount']:,}원 (평소 평균: {avg_30d:,}원) -> [위험 확률 {fraud_score}%]")
                print(f"  - 연동 피처: 10분간결제={streaming_features.get('tx_count_last_10m', 1)}회 | 대역폭 이상치 감지됨")
                print("-" * 80)
            else:
                print(f"✅ [람다 승인 완료] 유저: {user_id} | 금액: {tx_data['amount']:,}원 (평소 평균: {avg_30d:,}원) | Score: {fraud_score}점")

    except KeyboardInterrupt:
        print("\n🛑 FDS 서비스를 안전하게 셧다운합니다.")
    finally:
        db_cursor.close()
        db_conn.close()
