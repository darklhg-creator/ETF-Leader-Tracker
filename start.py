import requests
from pykrx import stock
import pandas as pd
from datetime import datetime, timedelta, timezone

WEBHOOK_URL = "https://discord.com/api/webhooks/1466732864392397037/roekkL5WS9fh8uQnm6Bjcul4C8MDo1gsr1ZmzGh8GfuomzlJ5vpZdVbCaY--_MZOykQ4"

def send_discord_message(msg_content):
    payload = {"content": msg_content}
    try:
        requests.post(WEBHOOK_URL, json=payload)
    except Exception as e:
        print(f"❌ 전송 에러: {e}")

def main():
    KST = timezone(timedelta(hours=9))
    today_dt = datetime.now(KST)
    target_date = today_dt.strftime("%Y%m%d")
    
    print(f"📅 조회 기준일: {target_date}")

    try:
        # 1. 데이터 가져오기
        df_today = stock.get_etf_ohlcv_by_ticker(target_date)
        
        if df_today.empty:
            print("❌ 오늘 데이터 자체가 없습니다.")
            return

        # 🔍 [디버깅] 현재 pykrx가 가져온 실제 컬럼명을 터미널에 출력
        actual_cols = df_today.columns.tolist()
        print(f"🔎 확인된 컬럼명: {actual_cols}")

        exclude_filters = [
            '미국', '차이나', '중국', '일본', '나스닥', 'S&P', '글로벌', 'MSCI', '인도', '베트남', 
            '필라델피아', '레버리지', '인버스', '블룸버그', '항셍', '니케이', '빅테크', 'TSMC', 
            '대만', '유로', '스톡스', '선물', '채권', '국고채', '머니마켓', 'KOFR', 'CD금리'
        ]
        
        results = []

        # 2. 컬럼 매칭 (이름에 포함된 단어로 찾기)
        # '등락'이 들어간 컬럼과 '대금'이 들어간 컬럼을 찾습니다.
        rate_col = next((c for c in actual_cols if '등락' in c), None)
        amt_col = next((c for c in actual_cols if '대금' in c), None)

        if not rate_col or not amt_col:
            print(f"❌ 필요한 컬럼을 찾지 못했습니다. (찾은 컬럼: {actual_cols})")
            return

        for ticker, row in df_today.iterrows():
            name = stock.get_etf_ticker_name(ticker)
            if any(word in name for word in exclude_filters): continue
            
            try:
                change_rate = float(row[rate_col])
                trading_amt = float(row[amt_col])
                
                # 상승한 종목만 수집
                if change_rate > 0:
                    results.append({
                        '종목명': name,
                        '상승률': change_rate,
                        '거래대금(억)': round(trading_amt / 100_000_000, 1)
                    })
            except:
                continue

        # 3. 결과 처리
        if results:
            final_df = pd.DataFrame(results).sort_values(by='상승률', ascending=False).head(10)
            final_df['상승률'] = final_df['상승률'].map(lambda x: f"{x:.2f}%")

            discord_msg = f"🚀 **[오늘의 국내 ETF 상승률 TOP 10]** ({today_dt.strftime('%Y-%m-%d')})\n"
            discord_msg += "```text\n"
            discord_msg += final_df.to_string(index=False) + "\n"
            discord_msg += "```\n"
            
            send_discord_message(discord_msg)
            print(final_df)
        else:
            print("⚠️ 필터링 후 결과가 0개입니다. (모두 하락했거나 제외 필터에 걸림)")

    except Exception as e:
        print(f"❌ 오류 상세: {e}")

if __name__ == "__main__":
    main()
