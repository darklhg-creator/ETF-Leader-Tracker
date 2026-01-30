import requests
import json
import pandas as pd
from pykrx import stock
from datetime import datetime, timedelta
import time

# ==========================================
# 1. 사용자 설정 (급등 후 거래량 급감 패턴)
# ==========================================
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1466732864392397037/roekkL5WS9fh8uQnm6Bjcul4C8MDo1gsr1ZmzGh8GfuomzlJ5vpZdVbCaY--_MZOykQ4"

TARGET_DATE = datetime.now().strftime("%Y%m%d")

# [A. 기준봉(폭발) 조건]
CHECK_DAYS = 30           # 최근 30일 이내 탐색
FLAG_PRICE_RATE = 10.0    # 10% 이상 주가 급등
FLAG_VOL_RATE = 5.0       # 전일 대비 거래량 500%(5배) 이상 폭발

# [B. 눌림목(침묵) 조건]
QUIET_VOL_RATIO = 0.25    # 기준봉 거래량의 25% 이하로 유지될 것

print(f"[{TARGET_DATE}] '폭발 후 침묵' 패턴 분석 시작")
print(f"조건: 30일내 {int(FLAG_PRICE_RATE)}%↑/5배 거래량 → 이후 거래량 {int(QUIET_VOL_RATIO*100)}% 이하 유지")
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
    """코스피 500위 + 코스닥 1000위"""
    print("1. 검색 대상 리스트 확보 중...")
    try:
        df_kospi = stock.get_market_cap(date, market="KOSPI")
        top_kospi = df_kospi.sort_values(by='시가총액', ascending=False).head(500).index.tolist()
        
        df_kosdaq = stock.get_market_cap(date, market="KOSDAQ")
        top_kosdaq = df_kosdaq.sort_values(by='시가총액', ascending=False).head(1000).index.tolist()
        
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
print("2. 패턴 매칭 시작...")

count = 0
for ticker in tickers:
    count += 1
    if count % 100 == 0: print(f"   ... {count}개 완료")

    try:
        # 데이터 넉넉히 60일치 가져오기
        start_date = (datetime.strptime(TARGET_DATE, "%Y%m%d") - timedelta(days=60)).strftime("%Y%m%d")
        ohlcv = stock.get_market_ohlcv_by_date(start_date, TARGET_DATE, ticker)
        
        if len(ohlcv) < 40: continue

        # 최근 30일 데이터 확인 (오늘 포함)
        recent_data = ohlcv.iloc[-(CHECK_DAYS+1):]
        
        found_trigger = False
        trigger_date = ""
        trigger_vol = 0
        trigger_price_change = 0.0
        
        # ---------------------------------------------------------
        # Step 1. 기준봉(Trigger) 찾기
        # ---------------------------------------------------------
        # 과거부터 오늘 전날까지만 탐색 (오늘은 눌림목이어야 하니까)
        # i는 recent_data 내의 인덱스
        for i in range(1, len(recent_data) - 1): # 첫날(전일비교불가)과 오늘(눌림확인용) 제외
            
            curr_vol = recent_data['거래량'].iloc[i]
            prev_vol = recent_data['거래량'].iloc[i-1]
            
            curr_close = recent_data['종가'].iloc[i]
            prev_close = recent_data['종가'].iloc[i-1]
            
            if prev_close == 0 or prev_vol == 0: continue
            
            # 조건 1: 주가 상승률 10% 이상
            price_rate = (curr_close - prev_close) / prev_close * 100
            
            # 조건 2: 거래량 전일대비 500% 이상 (5배)
            vol_rate = curr_vol / prev_vol
            
            if price_rate >= FLAG_PRICE_RATE and vol_rate >= FLAG_VOL_RATE:
                # 기준봉 발견!
                found_trigger = True
                trigger_date = recent_data.index[i].strftime("%Y-%m-%d")
                trigger_vol = curr_vol
                trigger_price_change = price_rate
                
                # 기준봉 이후의 데이터들 (눌림목 검증 대상)
                post_trigger_data = recent_data.iloc[i+1:]
                break # 가장 최근 기준봉 하나만 찾으면 됨 (또는 루프 돌면서 계속 확인도 가능하나 일단 첫 발견 기준)

        if not found_trigger:
            continue

        # ---------------------------------------------------------
        # Step 2. 눌림목(Quiet) 검증
        # ---------------------------------------------------------
        # 기준봉 이후 모든 날짜의 거래량이 기준봉의 25% 이하여야 함
        is_quiet = True
        current_vol_ratio = 0.0 # 오늘 거래량 비율
        
        for i in range(len(post_trigger_data)):
            daily_vol = post_trigger_data['거래량'].iloc[i]
            
            # 만약 하루라도 거래량이 기준봉의 25%를 넘으면 탈락
            # (단, 오늘 거래량이 살짝 넘는건 반등 시그널일 수 있으니 고려? -> 일단 사용자 조건대로 칼같이 제외)
            if daily_vol > (trigger_vol * QUIET_VOL_RATIO):
                is_quiet = False
                break
            
            # 마지막 날(오늘)의 거래량 비율 저장
            if i == len(post_trigger_data) - 1:
                current_vol_ratio = (daily_vol / trigger_vol) * 100

        if not is_quiet:
            continue
            
        # ---------------------------------------------------------
        # Step 3. 수급 정보 (보조지표)
        # ---------------------------------------------------------
        supply_start = (datetime.strptime(TARGET_DATE, "%Y%m%d") - timedelta(days=7)).strftime("%Y%m%d")
        supply_df = stock.get_market_net_purchases_of_equities_by_date(supply_start, TARGET_DATE, ticker)
        recent_supply = supply_df.tail(5)
        
        inst_sum = int(recent_supply['기관합계'].sum())
        for_sum = int(recent_supply['외국인'].sum())

        name = stock.get_market_ticker_name(ticker)
        
        results.append({
            '종목명': name,
            '현재가': ohlcv['종가'].iloc[-1],
            '기준일': trigger_date,
            '기준상승': f"{round(trigger_price_change,1)}%",
            '현재거래비율': f"{round(current_vol_ratio,1)}%",
            '기관수급': inst_sum,
            '외인수급': for_sum
        })

    except Exception as e:
        # print(f"Error {ticker}: {e}") # 디버깅용
        continue

# ==========================================
# 4. 결과 전송
# ==========================================
print("\n" + "="*70)
print(f"📊 분석 완료 ({len(results)}개 발견). 디스코드 전송...")

if len(results) > 0:
    res_df = pd.DataFrame(results)
    # 기준일이 최근인 순서대로 정렬 (가장 따끈따끈한 눌림목)
    res_df = res_df.sort_values(by='기준일', ascending=False)

    discord_msg = f"## 🌋 {TARGET_DATE} 폭발 후 침묵(눌림목) 발견\n"
    discord_msg += f"**조건:** 10%↑/5배 거래량 폭발 후 → 거래량 25%이하 유지\n\n"
    
    for idx, row in res_df.head(20).iterrows():
        icon = "🤫" # 조용함
        if row['기관수급'] > 0 and row['외인수급'] > 0: icon = "🔥"
        elif row['기관수급'] > 0: icon = "🔴"
        elif row['외인수급'] > 0: icon = "🔵"

        discord_msg += (
            f"**{idx+1}. {row['종목명']}** {icon}\n"
            f"> 가격: {row['현재가']:,}원\n"
            f"> 폭발: {row['기준일']} ({row['기준상승']})\n"
            f"> 침묵: 기준봉 대비 거래량 **{row['현재거래비율']}**\n"
            f"> 수급: 기 {row['기관수급']:,} / 외 {row['외인수급']:,}\n\n"
        )
    
    send_discord_message(DISCORD_WEBHOOK_URL, discord_msg)
    print("✅ 전송 완료!")

else:
    msg = f"## 📉 {TARGET_DATE} 분석 결과\n조건(폭발 후 거래량 급감)에 맞는 종목이 없습니다.\n기준봉 이후 거래량이 25% 이하로 유지되는 경우가 매우 드뭅니다."
    send_discord_message(DISCORD_WEBHOOK_URL, msg)
    print("검색된 종목 없음.")
