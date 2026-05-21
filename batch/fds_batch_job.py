import psycopg2
import redis

def run_batch_feature_engineering(base_date):
    """
    [Production Optimization]
    1. 멱등성 보장을 위해 실행 시점 기준 날짜(base_date)를 파라미터로 받아 처리함.
    2. OOM 방지를 위해 fetchall() 대신 서버 사이드 커서와 chunk 단위의 데이터 추출 메커니즘 채택.
    """
    print(f"🧹 [Batch Layer] 기준일 {base_date} 기준 장기 배치 피처 연산 워크플로우를 시작합니다...")
    
    conn = psycopg2.connect(host="localhost", database="fds_db", user="fds_user", password="fds_password", port="5432")
    
    # 서버사이드 커서 명명 선언을 통해 메모리에 한 번에 올리지 않고 DB 백엔드 버퍼 활용
    cursor = conn.cursor(name="fds_large_scale_batch_cursor")
    redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    
    # NOW() 하드코딩 제거, 파라미터 매핑을 통한 배치 멱등성 충족
    query = """
        SELECT 
            user_id,
            COALESCE(AVG(amount), 0) as avg_amount_30d,
            COALESCE(MAX(amount), 0) as max_amount_30d
        FROM transaction_logs
        WHERE tx_timestamp >= %s::timestamp - INTERVAL '30 days'
          AND tx_timestamp < %s::timestamp
        GROUP BY user_id;
    """
    
    try:
        cursor.execute(query, (base_date, base_date))
        chunk_size = 5000  # 메모리 안전 대역폭 한계 설정
        
        while True:
            rows = cursor.fetchmany(chunk_size)
            if not rows:
                break
                
            pipe = redis_client.pipeline()
            for row in rows:
                user_id = row[0]
                avg_amount = int(row[1])
                max_amount = int(row[2])
                
                batch_key = f"fds:user:{user_id}:batch"
                pipe.hset(batch_key, mapping={
                    "avg_amount_30d": str(avg_amount),
                    "max_amount_30d": str(max_amount)
                })
            pipe.execute()
            print(f"📊 {len(rows)}개 유저 레코드 청크 파이프라인 동기화 완료.")
            
        print("✅ [Batch Layer] 배치 피처 데이터가 Redis Feature Store에 안착되었습니다.")
        
    except Exception as e:
        print(f"❌ 배치 연산 실패: {e}")
    finally:
        cursor.close()
        conn.close()
