import streamlit as st
from sqlalchemy import create_engine
import pandas as pd
import plotly.express as px
import time

# 페이지 기본 설정
st.set_page_config(
    page_title="실시간 FDS 모니터링 대시보드",
    page_icon="🛡️",
    layout="wide"
)

# [최적화] Pandas 경고 해결을 위해 SQLAlchemy 커넥션 풀 엔진 정의
@st.cache_resource
def get_db_engine():
    return create_engine("postgresql+psycopg2://fds_user:fds_password@localhost:5432/fds_db")

engine = get_db_engine()

# 메인 타이틀
st.title("🛡️ Real-time Fraud Detection System Dashboard")
st.markdown("PostgreSQL의 트랜잭션 로그를 1초 주기로 동적 스캐닝하여 FDS 판정 현황을 시각화합니다.")

# 실시간 루프 제어를 위한 Placeholder 선언
metrics_placeholder = st.empty()
charts_placeholder = st.empty()
table_placeholder = st.empty()

# 중복 ID 에러 방지용 루프 카운터 초기화
iteration = 0

while True:
    try:
        iteration += 1
        
        # 1. SQLAlchemy 엔진을 활용해 경고 없이 안전하게 최신 로그 긁어오기
        query = """
            SELECT tx_id, user_id, tx_timestamp, amount, merchant_category, device_id, fraud_score, decision 
            FROM transaction_logs 
            ORDER BY tx_timestamp DESC 
            LIMIT 500;
        """
        df = pd.read_sql(query, engine)
        
        if not df.empty:
            # 2. 최상단 핵심 메트릭 카드 연산
            total_tx = len(df)
            blocked_df = df[df['decision'] == 'BLOCKED']
            approved_df = df[df['decision'] == 'APPROVED']
            
            blocked_count = len(blocked_df)
            fraud_rate = (blocked_count / total_tx) * 100 if total_tx > 0 else 0.0
            
            with metrics_placeholder.container():
                col1, col2, col3, col4 = st.columns(4)
                col1.metric("총 처리 트랜잭션 (최근 500건)", f"{total_tx:,} 건")
                col2.metric("정상 승인 (APPROVED)", f"{len(approved_df):,} 건")
                col3.metric("이상 차단 (BLOCKED)", f"{blocked_count:,} 건", delta=f"+{blocked_count}", delta_color="inverse")
                col4.metric("실시간 사기 탐지율 (Fraud Rate)", f"{fraud_rate:.1f} %")
            
            # 3. 차트 레이어 구성 (승인 vs 차단 비율 & 가맹점별 차단 분포)
            with charts_placeholder.container():
                chart_col1, chart_col2 = st.columns(2)
                
                # 원형 차트: 승인/차단 비율 (width='stretch' 최신 문법 반영)
                fig_pie = px.pie(
                    df, names='decision', title="💡 실시간 거래 승인 vs 차단 비율",
                    color='decision', color_discrete_map={'APPROVED': '#2ecc71', 'BLOCKED': '#e74c3c'}
                )
                chart_col1.plotly_chart(fig_pie, width='stretch', key=f"pie_chart_{iteration}")
                
                # 바 차트: 고위험 가맹점 업종별 분포 (width='stretch' 최신 문법 반영)
                if blocked_count > 0:
                    fig_bar = px.bar(
                        blocked_df, x='merchant_category', title="🔥 사기 거래 탐지 가맹점 분포",
                        labels={'merchant_category': '가맹점 카테고리'},
                        color_discrete_sequence=['#e67e22']
                    )
                    chart_col2.plotly_chart(fig_bar, width='stretch', key=f"bar_chart_{iteration}")
                else:
                    chart_col2.info("현재 차단된 사기 거래 가맹점 데이터가 없습니다.")
            
            # 4. 최신 사기 거래(BLOCKED) 실시간 알림판 테이블
            with table_placeholder.container():
                st.subheader("🚨 실시간 사기(Fraud) 의심 거래 실시간 관제판")
                if not blocked_df.empty:
                    display_blocked = blocked_df[['tx_timestamp', 'user_id', 'amount', 'merchant_category', 'device_id', 'fraud_score']].copy()
                    display_blocked['amount'] = display_blocked['amount'].apply(lambda x: f"{x:,}원")
                    display_blocked['fraud_score'] = display_blocked['fraud_score'].apply(lambda x: f"{x}% 확률")
                    
                    # 데이터프레임 구조 최신 문법 반영
                    st.dataframe(display_blocked.head(10), width='stretch', key=f"data_table_{iteration}")
                else:
                    st.success("현재 시스템이 완벽하게 안전합니다. 탐지된 위험 거래가 없습니다.")
                    
        else:
            st.info("데이터베이스에 적재된 트랜잭션 로그가 없습니다. Generator와 Inference 엔진을 가동해 주세요.")
            
    except Exception as e:
        st.error(f"대시보드 런타임 에러 발생: {e}")
        
    # 1초 대기 후 루프 재실행
    time.sleep(1)
