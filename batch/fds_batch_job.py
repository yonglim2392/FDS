import psycopg2
import redis

def run_batch_feature_engineering():
    print("🧹 [Batch Layer] 장기 배치 피처 연산 워크플로우를 시작합니다...")
    
    # 인프라 커넥션
    conn = psycopg2.connect(host="localhost", database="fds_db", user="fds_user", password="fds_password", port="5432")
    cursor = conn.cursor()
    redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    
    # 최근 30일간 유저별 평균 결제 금액과 최대 결제 금액을 집계하는 대량 배치 쿼리
    query = """
        SELECT 
            user_id,
            COALESCE(AVG(amount), 0) as avg_amount_30d,
            COALESCE(MAX(amount), 0) as max_amount_30d
        FROM transaction_logs
        WHERE tx_timestamp >= NOW() - INTERVAL '30 days'
        GROUP BY user_id;
    """
    
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        
        print(f"📊 총 {len(rows)}명의 유저에 대한 장기 프로필 피처 연산 완료. Redis 동기화 중...")
        
        # Redis 파이프라인을 활용해 대량 데이터를 벌크(Bulk)로 초고속 주입
        pipe = redis_client.pipeline()
        for row in rows:
            user_id = row[0]
            avg_amount = int(row[1])
            max_amount = int(row[2])
            
            # Key 구조 분리: fds:user:{user_id}:batch
            batch_key = f"fds:user:{user_id}:batch"
            pipe.hset(batch_key, mapping={
                "avg_amount_30d": str(avg_amount),
                "max_amount_30d": str(max_amount)
            })
        pipe.execute()
        print("✅ [Batch Layer] 30일 기준 배치 피처 데이터가 Redis Feature Store에 안착되었습니다.")
        
    except Exception as e:
        print(f"❌ 배치 연산 실패: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    run_batch_feature_engineering()
