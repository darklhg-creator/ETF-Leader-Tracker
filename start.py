import requests
from pykrx import stock
import pandas as pd
from datetime import datetime, timedelta, timezone

# ==========================================
# ⚙️ 1. 환경 설정 (Configuration)
# ==========================================
WEBHOOK_URL = "https://discord.com/api/webhooks/1466732864392397037/roekkL5WS9fh8uQnm6Bjcul4C8MDo1gsr1ZmzGh8GfuomzlJ5vpZdVbCaY--_MZOykQ4"

EXCLUDE_KEYWORDS = [
    '미국', '차이나', '중국', '일본', '나스닥', 'S&P', '글로벌', 'MSCI', '인도', '베트남', 
    '필라델피아', '레버리지', '인버스', '블룸버그', '항셍', '니케이', '빅테크', 'TSMC', 
    '대만', '유로', '스톡스', '선물', '채권', '국고채', '머니마켓', 'KOFR', 'CD금리', '달러', '엔화'
]

# ==========================================
# 🛠️ 2. 핵심 기능 클래스 (ETF Data Pipeline)
# ==========================================
class ETFTracker:
    def __init__(self, target_date):
        self.target_date = target_date
        self.df = pd.DataFrame()

    def fetch_data(self):
        # 1. 영업일 목록 조회하여 오늘과 직전 거래일 정확히 찾기
        dt_end = datetime.strptime(self.target_date, "%Y%m%d")
        dt_start = dt_end - timedelta(days=10)
        
        b_days = stock.get_business_days_dates(dt_start.strftime("%Y%m%d"), self.target_date)
        
        if len(b_days) < 2:
            raise ValueError("영업일 데이터가 부족합니다.")
            
        curr_date = b_days[-1].strftime("%Y%m%d")
        prev_date = b_days[-2].strftime("%Y%m%d")
        
        print(f"📡 수집 기준일: {curr_date} / 비교일(전일): {prev_date}")
        
        # 2. 오늘과 전일의 시세 데이터를 각각 통째로 수집 (등락률이 없어도 OK)
        df_curr = stock.get_etf_ohlcv_by_ticker(curr_date)
        df_prev = stock.get_etf_ohlcv_by_ticker(prev_date)
        
        if df_curr.empty or df_prev.empty:
            raise ValueError("데이터를 불러오지 못했습니다.")
            
        # 3. Pandas Join 연산을 통한 고속 병합 및 자체 등락률 계산
        # 인덱스(티커) 기준으로 두 데이터를 완벽하게 매칭시켜 숫자가 꼬이지 않음
        df_merged = df_curr[['종가', '거래대금']].join(df_prev[['종가']], lsuffix='_현재', rsuffix='_전일')
        
        # 자체 계산식: ((오늘종가 - 어제종가) / 어제종가) * 100
        df_merged['등락률'] = ((df_merged['종가_현재'] - df_merged['종가_전일']) / df_merged['종가_전일']) * 100
        
        # 4. 종목명 추가
        df_merged['종목명'] = [stock.get_etf_ticker_name(t) for t in df_merged.index]
        
        self.df = df_merged
        print(f"✅ 수집 및 연산 완료 (총 {len(self.df)}개 종목)")
        
    def process_data(self):
        df = self.df.copy()
        
        # 1. 제외 키워드 필터링 (고속 문자열 연산)
        pattern = '|'.join(EXCLUDE_KEYWORDS)
        df = df[~df['종목명'].str.contains(pattern, na=False)]
        
        # 2. 신규 상장 등으로 전일 데이터가 없어 등락률이 NaN인 종목 제거
        df = df.dropna()
        
        # 3. 상승률 0% 초과 종목만 필터링 후 정렬
        top10_df = df[df['등락률'] > 0].sort_values(by='등락률', ascending=False).head(10)
        
        # 4. 깔끔한 출력을 위한 리스트 조립
        results = []
        for _, row in top10_df.iterrows():
            results.append({
                '종목명': row['종목명'],
                '상승률(%)': float(row['등락률']),
                '거래대금(억)': round(float(row['거래대금_현재']) / 100_000_000, 1)
            })
            
        return pd.DataFrame(results)

# ==========================================
# 🚀 3. 디스코드 전송 및 메인 실행
# ==========================================
def send_discord(df_result, target_date):
    if df_result.empty:
        msg = f"⚠️ **[{target_date}]** 조건에 맞는 상승 종목이 없습니다."
    else:
        df_display = df_result.copy()
        df_display['상승률(%)'] = df_display['상승률(%)'].apply(lambda x: f"{x:.2f}%")
        
        msg = f"🚀 **[국내 주도주 ETF 상승률 TOP 10]** ({target_date})\n"
        msg += "```text\n"
        msg += df_display.to_string(index=False) + "\n"
        msg += "```\n"
        msg += "💡 Pandas 자체 병합 연산을 적용하여 정확도와 속도를 극대화했습니다."

    try:
        requests.post(WEBHOOK_URL, json={"content": msg})
        print("✉️ 디스코드 메시지 전송 성공!")
    except Exception as e:
        print(f"❌ 전송 실패: {e}")

def main():
    KST = timezone(timedelta(hours=9))
    today = datetime.now(KST)
    
    if today.weekday() >= 5:
        print("💤 주말입니다. 분석을 쉬어갑니다.")
        return

    target_date = today.strftime("%Y%m%d")
    display_date = today.strftime("%Y-%m-%d")

    try:
        tracker = ETFTracker(target_date)
        tracker.fetch_data()
        final_df = tracker.process_data()
        
        print("\n📊 [분석 결과]")
        print(final_df)
        
        send_discord(final_df, display_date)

    except Exception as e:
        error_msg = f"❌ 시스템 에러: {e}"
        print(error_msg)
        requests.post(WEBHOOK_URL, json={"content": error_msg}) 

if __name__ == "__main__":
    main()
