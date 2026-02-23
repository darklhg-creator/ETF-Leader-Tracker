import requests
from pykrx import stock
import pandas as pd
from datetime import datetime, timedelta, timezone

# 🔴 디스코드 웹후크 URL
WEBHOOK_URL = "https://discord.com/api/webhooks/1466732864392397037/roekkL5WS9fh8uQnm6Bjcul4C8MDo1gsr1ZmzGh8GfuomzlJ5vpZdVbCaY--_MZOykQ4"

def send_discord_message(msg_content):
    payload = {"content": msg_content}
    try:
        response = requests.post(WEBHOOK_URL, json=payload)
        if response.status_code == 204:
            print("✅ 디스코드 알림 전송 완료!")
        else:
            print(f"⚠️ 디스코드 전송 실패 (상태 코드: {response.status_code})")
    except Exception as e:
        print(f"❌ 디스코드 전송 중 에러 발생: {e}")

def main():
    KST = timezone(timedelta(hours=9))
    today_dt = datetime.now(KST)
    target_date = today_dt.strftime("%Y%m%d")
    # 20일 평균 대금을 구하기 위해 충분한 기간 설정
    start_date = (today_dt - timedelta(days=50)).strftime("%Y%m%d")
    
    print(f"📅 실행일시: {today_dt.strftime('%Y-%m-%d %H:%M:%S')} (KST)")

    if today_dt.weekday() >= 5:
        send_discord_message(f"💤 **[{today_dt.strftime('%Y-%m-%d')}]** 오늘은 주말입니다. 쉬어갑니다!")
        return 
    
    try:
        # 1. 오늘 전체 ETF 시세 및 등락률 가져오기
        df_today = stock.get_etf_ohlcv_by_ticker(target_date)
        
        if df_today.empty:
            send_discord_message(f"💤 **[{today_dt.strftime('%Y-%m-%d')}]** 휴장일로 판단됩니다.")
            return

        exclude_filters = [
            '미국', '차이나', '중국', '일본', '나스닥', 'S&P', '글로벌', 'MSCI', '인도', '베트남', 
            '필라델피아', '레버리지', '인버스', '블룸버그', '항셍', '니케이', '빅테크', 'TSMC', 
            '대만', '유로', '스톡스', '선물', '채권', '국고채', '머니마켓', 'KOFR'
        ]
        
        results = []
        
        # 2. 필터링 및 데이터 수집
        for ticker, row in df_today.iterrows():
            name = stock.get_etf_ticker_name(ticker)
            
            # 해외/파생/채권형 제외
            if any(word in name for word in exclude_filters): continue
            
            # 상승한 종목만 대상 (등락률 > 0)
            change_rate = row['등락률']
            if change_rate <= 0: continue
            
            # 오늘 거래대금 (10억 이상인 것만 필터링 - 너무 잡주 제외)
            today_amt = row['거래대금']
            if today_amt < 1_000_000_000: continue

            # 3. 20일 평균 거래대금 계산
            df_past = stock.get_market_ohlcv_by_date(start_date, target_date, ticker)
            if len(df_past) < 10: continue
            
            avg_amt_20 = (df_past['종가'] * df_past['거래량']).iloc[:-1].tail(20).mean()

            results.append({
                '종목명': name,
                '상승률': f"{change_rate:.2f}%",
                '오늘대금(억)': round(today_amt / 100_000_000, 1),
                '20일평균대금(억)': round(avg_amt_20 / 100_000_000, 1),
                '_raw_rate': change_rate # 정렬용 숫자 데이터
            })

        # 4. 상승률 기준 TOP 10 정렬
        if results:
            final_df = pd.DataFrame(results).sort_values(by='_raw_rate', ascending=False).head(10)
            final_df = final_df.drop(columns=['_raw_rate']) # 정렬용 컬럼 삭제

            # 디스코드 메시지 생성
            discord_msg = f"🚀 **[오늘의 국내 상승 주도 ETF TOP 10]** ({today_dt.strftime('%Y-%m-%d')})\n"
            discord_msg += "```text\n"
            discord_msg += final_df.to_string(index=False) + "\n"
            discord_msg += "```\n"
            discord_msg += "💡 위 ETF들의 구성 종목을 확인하여 주도 섹터를 분석해 보세요!"
            
            send_discord_message(discord_msg)
            print(final_df)
        else:
            print("조건에 맞는 상승 종목이 없습니다.")

    except Exception as e:
        print(f"❌ 오류 발생: {e}")

if __name__ == "__main__":
    main()
