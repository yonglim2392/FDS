import json
import redis
import psycopg2
import joblib
import pandas as pd
import queue
import threading
from kafka import KafkaConsumer

# 인프라 자원 초기화
redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
model = joblib.load('model/fds_model.pkl')

# [Production Optimization] 비동기 쓰기 버퍼를 위한 인메모리 큐 아키텍처 도입
db_write_queue = queue.Queue(maxsize=10000)

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='fds-inference-group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

def db_worker_thread():
    """[Data Engineering Pattern] 백그라운드에서 큐를 컨슘하여 DB 디스크에 비동기로 쓰기를 수행하는 독립 작업 레이어"""
    db_conn = psycopg2.connect(host="localhost", database="fds_db", user="fds_user", password="fds_password", port="5432")
    db_cursor = db_conn.cursor()
    
    query = """
        INSERT INTO transaction_logs 
        (tx_id, user_id, tx_timestamp, amount, merchant_category, device_id, ip_address, fraud_score, decision, tx_count_10m, total_amount_10m)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    while True:
        item = db_write_queue.get()
        if item is None:  # 종료 시그널
            break
        try:
            tx_data, fraud_score, decision, tx_count_10m, total_amount_10m = item
            db_cursor.execute(query, (
                tx_data['tx_id'], tx_data['user_id'], tx_data['timestamp'], tx_data['amount'],
                tx_data['merchant_category'], tx_data['device_id'], tx_data['ip_address'],
                fraud_score, decision, tx_count_10m, total_amount_10m
            ))
            db_conn.commit()
        except Exception as e:
            print(f"❌ DB 비동기 적재 에러: {e}")
            db_conn.rollback()
        finally:
            db_write_queue.task_done()
            
    db_cursor.close()
    db_conn.close()

# 백그라운드 데이터베이스 적재 스레드 데몬 시작
worker = threading.Thread(target=db_worker_thread, daemon=True)
worker.start()

def predict_lambda_fds(tx_data, streaming_features, batch_features):
    current_amount = tx_data['amount']
    tx_count_10m = int(streaming_features.get('tx_count_last_10m', 0))
    total_amount_10m = int(streaming_features.get('total_amount_last_10m', 0))
    avg_amount_30d = int(batch_features.get('avg_amount_30d', current_amount))
    
    spending_ratio_vs_30d = float(current_amount / avg_amount_30d) if avg_amount_30d > 0 else 1.0
    is_high_risk_cat = 1 if tx_data.get('merchant_category') in ["game_money", "gift_card"] else 0
    is_hacker_dev = 1 if tx_data.get('device_id') == "dev_unknown_hacker" else 0
    
    input_df = pd.DataFrame([{
        'amount': current_amount,
        'tx_count_10m': tx_count_10m,
        'total_amount_10m': total_amount_10m,
        'is_high_risk_cat': is_high_risk_cat,
        'is_hacker_dev': is_hacker_dev
    }])
    
    fraud_probability = model.predict_proba(input_df)[0][1]
    fraud_score = int(fraud_probability * 100)
    
    if spending_ratio_vs_30d >= 10.0 and current_amount >= 100000:
        fraud_score = max(fraud_score, 85)
        
    return fraud_score, avg_amount_30d

if __name__ == "__main__":
    print("🛡️ [완전판 람다 FDS] 비동기 적재 엔진 일체형 인라인 추론 시작...")
    print("-" * 80)
    
    try:
        for message in consumer:
            tx_data = message.value
            user_id = tx_data['user_id']
            
            stream_key = f"fds:user:{user_id}"
            streaming_features = redis_client.hgetall(stream_key)
            
            batch_key = f"fds:user:{user_id}:batch"
            batch_features = redis_client.hgetall(batch_key)
            
            fraud_score, avg_30d = predict_lambda_fds(tx_data, streaming_features, batch_features)
            decision = "BLOCKED" if fraud_score >= 70 else "APPROVED"
            
            # [핵심 변경지점] 디스크 쓰기 병목을 막기 위해 동기 쿼리를 날리지 않고, 
            # 메모리 큐에 데이터를 인큐잉한 뒤 즉각 다음 메시지를 컨슘하러 루프 이동
            tx_count_10m = int(streaming_features.get('tx_count_last_10m', 0))
            total_amount_10m = int(streaming_features.get('total_amount_last_10m', 0))
            db_write_queue.put((tx_data, fraud_score, decision, tx_count_10m, total_amount_10m))
            
            if decision == "BLOCKED":
                print(f"🚨 [람다 차단 조치] 유저: {user_id} | 금액: {tx_data['amount']:,}원 (평소 평균: {avg_30d:,}원) -> [위험 확률 {fraud_score}%]")
                print("-" * 80)
            else:
                print(f"✅ [람다 승인 완료] 유저: {user_id} | 금액: {tx_data['amount']:,}원 (평소 평균: {avg_30d:,}원) | Score: {fraud_score}점")

    except KeyboardInterrupt:
        print("\n🛑 FDS 서비스를 안전하게 셧다운합니다.")
    finally:
        db_write_queue.put(None) # 작업 스레드 종료 시그널 전달
        worker.join()
