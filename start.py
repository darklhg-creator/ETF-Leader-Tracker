import pandas as pd
import numpy as np
from pykrx import stock
import time
from datetime import datetime, timedelta
import requests

def get_local_minima(series, order=5):
    minima_indices = []
    for i in range(order, len(series) - order):
        if all(series[i] <= series[i-j] for j in range(1, order + 1)) and \
           all(series[i] <= series[i+j] for j in range(1, order + 1)):
            minima_indices.append(i)
    return minima_indices

def check_linear_trend(ticker, name, start_date, end_date):
    try:
        df = stock.get_market_ohlcv_by_date(fromdate=start_date, todate=end_date, ticker=ticker)
        if len(df) < 30: return None

        # 1. 20일선 이격도 계산 (종가 기준)
        ma20 = df['종가'].rolling(window=20).mean()
        curr_disparity_20 = round((df['종가'].iloc[-1] / ma20.iloc[-1]) * 100, 1)

        # 2. 저점(저가) 기반 추세선 분석
        low_values = df['저가'].values
        low_idx = get_local_minima(low_values, order=5)
        if len(low_idx) > 0 and low_idx[-1] == len(df) - 1: low_idx = low_idx[:-1]

        if len(low_idx) >= 3:
            recent_x = np.array(low_idx[-3:])
            recent_y = low_values[recent_x]
            
            # 저점 상승 확인
            if not (recent_y[0] < recent_y[1] < recent_y[2]): return None

            # R2 신뢰도 계산
            coeffs = np.polyfit(recent_x, recent_y, 1)
            p = np.poly1d(coeffs)
            y_hat = p(recent_x); y_bar = np.mean(recent_y)
            ss_res = np.sum((recent_y - y_hat)**2); ss_tot = np.sum((recent_y - y_bar)**2)
            r_squared = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0
            if r_squared < 0.85: return None

            # 추세선 지지 확인
            today_idx = len(df) - 1
            expected_price = p(today_idx)
            current_close = df['종가'].iloc[-1]
            if not (expected_price * 0.99 <= current_close <= expected_price * 1.05): return None

            # 3. 저점 날짜 포맷팅 (MM/DD)
            low_dates = [df.index[i].strftime("%m/%d") for i in recent_x]

            return {
                "종목명": name,
                "1차저점": low_dates[0],
                "2차저점": low_dates[1],
                "3차저점": low_dates[2],
                "이격도": curr_disparity_20
            }
    except: pass
    return None

def is_market_open():
    now = datetime.now()
    if now.weekday() >= 5: return False
    target_date = now.strftime("%Y%m%d")
    try:
        df = stock.get_market_ohlcv_by_date(target_date, target_date, "005930")
        return not df.empty
    except: return False

def send_discord_message(content):
    webhook_url = "https://discord.com/api/webhooks/1466732864392397037/roekkL5WS9fh8uQnm6Bjcul4C8MDo1gsr1ZmzGh8GfuomzlJ5vpZdVbCaY--_MZOykQ4"
    requests.post(webhook_url, json={"content": content})

if __name__ == "__main__":
    if not is_market_open(): exit()

    now = datetime.now()
    all_targets = list(stock.get_market_cap_by_ticker(now.strftime("%Y%m%d"), market="KOSPI").sort_values(by='시가총액', ascending=False).head(500).index) + \
                  list(stock.get_market_cap_by_ticker(now.strftime("%Y%m%d"), market="KOSDAQ").sort_values(by='시가총액', ascending=False).head(1000).index)
    
    results = []
    for ticker in all_targets:
        name = stock.get_market_ticker_name(ticker)
        res = check_linear_trend(ticker, name, (now - timedelta(days=120)).strftime("%Y%m%d"), now.strftime("%Y%m%d"))
        if res: results.append(res)
        time.sleep(0.02)

    if results:
        final_df = pd.DataFrame(results).sort_values(by='이격도', ascending=False)
        msg = f"📅 {now.strftime('%Y-%m-%d')} 분석 결과\n```\n{final_df.to_string(index=False)}\n```"
    else:
        msg = f"📅 {now.strftime('%Y-%m-%d')} 포착된 종목이 없습니다."
    
    send_discord_message(msg)
