import requests
import json
import pandas as pd
from pykrx import stock
from datetime import datetime, timedelta
import time

# ==========================================
# 1. 사용자 설정 (마크로젠 사냥용 세팅)
# ==========================================
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1466732864392397037/roekkL5WS9fh8uQnm6Bjcul4C8MDo1gsr1ZmzGh8GfuomzlJ5vpZdVbCaY--_MZOykQ4"

TARGET_DATE = datetime.now().strftime("%Y%m%d")

# [A. 기준봉 조건: 과거에 힘을 썼는가?]
CHECK_PAST_DAYS = 15     # 최근 15일 이내에
BIG_RISE_THRESHOLD = 15.0 # 15% 이상 급등한(고가 기준) 날이 있어야 함

# [B. 눌림목 조건: 지금은 쉬고 있는가?]
MA_WINDOW = 20           # 20일선 기준
MIN_DISPARITY = 95.0     # 20일선 살짝 깨도 인정 (95% 이상)
MAX_DISPARITY = 110.0    # 20일선 위 (110% 이하)
VOL_DROP_RATE = 1.0      # 거래량이 전일보다 줄었거나 같으면 통과 (1.0 이하)

# [C. 수급 조건]
SUPPLY_CHECK_DAYS = 5    # 최근 5일 수급 합계

print(f"[{TARGET_DATE}] '급등 후 눌림목(N자 패턴)' 분석 시작 (시총 상위 1000개)")
print(f"조건: 최근 {CHECK_PAST_DAYS}일내 {BIG_RISE_THRESHOLD}%급등 + 거래량감소 + 20일선지지")
print("-" * 60)

# ==========================================
# 2. 함수 정의
# ==========================================
def send_discord_message(webhook_url, content):
    data = {"content": content}
    headers = {"Content-Type": "application/json"}
    try:
        requests.post(webhook_url, data=json.dumps(data), headers=headers)
    except:
        pass

def get_target_tickers(date):
    """코스피/코스닥 시총 상위 500개씩 (ETF 제외)"""
    print("1. 우량주 리스트 확보 중...")
    try:
        df_kospi = stock.get_market_cap(date, market="KOSPI")
        top_kospi = df_kospi.sort_values(by='시가총액', ascending=False).head(500).index.tolist()
        
        df_kosdaq = stock.get_market_cap(date, market="KOSDAQ")
        top_kosdaq = df_kosdaq.sort_values(by='시가총액', ascending=False).head(500).index.tolist()
        
        total_tickers = top_kospi + top_kosdaq
        
        etfs = stock.get_etf_ticker_list(date)
        etns = stock.get_etn_ticker_list(date)
        exclude_list = set(etfs + etns)
        
        return [t for t in total_tickers if t not in exclude_list]
    except:
        return []

# ==========================================
# 3. 메인 분석 로직
# ==========================================
tickers = get_target_tickers(TARGET_DATE)
print(f"   -> 분석 대상: {len(tickers)}개 종목")

results = []
print("2. 기준봉 및 눌림목 패턴 분석 시작...")

count = 0
for ticker in tickers:
    count += 1
    if count % 100 == 0: print(f"   ... {count}개 완료")

    try:
        # 데이터 넉넉히 가져오기 (이평선 + 과거 탐색용)
        start_date = (datetime.strptime(TARGET_DATE, "%Y%m%d") - timedelta(days=60
