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
    
    print(f"📅 조회 기준일: {target_date}")

    try:
        # 1. 오늘 전체 ETF 시세 가져오기
        df_today = stock.get_etf_ohlcv_by_ticker(target_date)
        
        if df_today.empty:
            send_discord_message(f"⚠️ [{target_date}] 데이터를 불러올 수 없습니다. 장마감 데이터 집계 중일 수 있습니다.")
            return

        exclude_filters = [
            '미국', '차이나', '중국', '일본', '나스닥', 'S&P', '글로벌', 'MSCI', '인도', '베트남', 
            '필라델피아', '레버리지', '인버스', '블룸버그', '항셍', '니케이', '빅테크', 'TSMC', 
            '대만', '유로', '스톡스', '선물', '채권', '국고채', '머니마켓', 'KOFR', 'CD금리'
        ]
        
        results = []
        
        for ticker, row in df_today.iterrows():
            name = stock.get_etf_ticker_name(ticker)
            if any(word in name for word in exclude_filters): continue
            
            # [수정] 컬럼명 대신 위치(iloc)로 안전하게 데이터 추출
            # 보통 pykrx ETF OHLCV의 등락률은 마지막에서 두 번째 혹은 특정 위치에 있습니다.
            try:
                # 등락률 컬럼이 있으면 사용, 없으면 직접 계산하거나 위치로 시도
                change_rate = row['등락률'] if '등락률' in df_today.columns else row.iloc[-2]
                trading_amt = row['거래대금'] if '거래대금' in df_today.columns else row.iloc[-1]
            except:
                continue
            
            results.append({
                '종목명': name,
                '상승률': float(change_rate),
                '거래대금(억)': round(float(trading_amt) / 100_000_000, 1)
            })

        if results:
            # 2. 상승률 기준 정렬 및 상위 10개
            final_df = pd.DataFrame(results).sort_values(by='상승률', ascending=False).head(10)
            
            # 출력용 포맷팅
            final_df['상승률'] = final_df['상승률'].map(lambda x: f"{x:.2f}%")

            discord_msg = f"🚀 **[오늘의 국내 ETF 상승률 TOP 10]** ({today_dt.strftime('%Y-%m-%d')})\n"
            discord_msg += "```text\n"
            discord_msg += final_df.to_string(index=False) + "\n"
            discord_msg += "```\n"
            discord_msg += "💡 국내 순수 섹터 중 가장 탄력이 좋았던 종목들입니다."
            
            send_discord_message(discord_msg)
        else:
            print("결과가 없습니다.")

    except Exception as e:
        # 에러 발생 시 상세 정보 출력
        error_msg = f"❌ 오류 발생: {e}"
        print(error_msg)
        # 에러 내용도 디코로 보내서 바로 확인할 수 있게 함
        # send_discord_message(error_msg) 

if __name__ == "__main__":
    main()
