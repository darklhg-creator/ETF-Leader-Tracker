import FinanceDataReader as fdr
from pykrx import stock
import requests
import pandas as pd
from datetime import datetime
import os

# 디스코드 설정
IGYEOK_WEBHOOK_URL = "https://discord.com/api/webhooks/1461902939139604684/ZdCdITanTb3sotd8LlCYlJzSYkVLduAsjC6CD2h26X56wXoQRw7NY72kTNzxTI6UE4Pi"

def send_discord_message(content):
    """디스코드 메시지 전송 (2000자 제한 대응)"""
    if not content or len(content.strip()) < 10: return
    if len(content) > 1900:
        chunks = [content[i:i+1900] for i in range(0, len(content), 1900)]
        for chunk in chunks:
            requests.post(IGYEOK_WEBHOOK_URL, json={'content': chunk})
    else:
        requests.post(IGYEOK_WEBHOOK_URL, json={'content': content})

def get_investor_data(code):
    """당일 기관, 외국인, 연기금 수급 데이터 가져오기 (주수 기준)"""
    today = datetime.now().strftime("%Y%m%d")
    try:
        df = stock.get_market_net_purchases_of_equities_by_ticker(today, today, "ALL")
        if code in df.index:
            inst = df.loc[code, '기관합계']
            fore = df.loc[code, '외국인합계']
            pension = df.loc[code, '연기금등']
            return inst, fore, pension
        return 0, 0, 0
    except:
        return 0, 0, 0

def main():
    print("🚀 [분석 시작] 4단계 자동 리포트 생성")
    
    try:
        # 데이터 로드
        df_krx = fdr.StockListing('KRX')
        df_kospi = df_krx[df_krx['Market']=='KOSPI'].head(500)
        df_kosdaq = df_krx[df_krx['Market']=='KOSDAQ'].head(500)
        target_codes = pd.concat([df_kospi, df_kosdaq])

        all_analyzed = []
        print(f"📡 {len(target_codes)}개 종목 이격도 스캔 중...")

        for idx, row in target_codes.iterrows():
            code = row['Code']
            name = row['Name']
            sector = row.get('Sector', '기타 업종')
            # 최신 공시 기준 영업이익 (0보다 크면 흑자)
            operating_profit = row.get('OperatingProfit', 0)

            try:
                df = fdr.DataReader(code).tail(30)
                if len(df) < 20: continue
                
                current_price = df['Close'].iloc[-1]
                ma20 = df['Close'].rolling(window=20).mean().iloc[-1]
                
                if ma20 == 0 or pd.isna(ma20): continue
                disparity = round((current_price / ma20) * 100, 1)

                # 1차 필터링: 이격도 95% 이하
                if disparity <= 95.0:
                    all_analyzed.append({
                        'name': name, 'code': code, 'disparity': disparity, 
                        'sector': sector, 'is_profit': operating_profit > 0
                    })
            except:
                continue

        if not all_analyzed:
            send_discord_message("🔍 조건에 맞는 종목이 없습니다.")
            return

        # --- 1번 메시지: 기존 이격도 분석 결과 ---
        results_95 = sorted(all_analyzed, key=lambda x: x['disparity'])
        report1 = "### 📊 1. 이격도 분석 결과 (95% 이하)\n"
        for r in results_95[:50]:
            report1 += f"· **{r['name']}({r['code']})**: {r['disparity']}%\n"
        send_discord_message(report1)

        # --- 2번 메시지: 1번 기업들 테마분류표 ---
        report2 = "### 📋 2. 1번 기업들 테마분류표\n"
        report2 += "| 테마(업종) | 종목명 | 이격도 |\n| --- | --- | --- |\n"
        results_sector = sorted(all_analyzed, key=lambda x: x['sector'])
        for r in results_sector[:40]:
            report2 += f"| {r['sector']} | {r['name']} | {r['disparity']}% |\n"
        send_discord_message(report2)

        # --- 3번 메시지: 2번 표에서 적자기업 제외 표 ---
        profit_only = [r for r in all_analyzed if r['is_profit']]
        report3 = "### 📉 3. 흑자기업 필터링 리스트 (적자 제외)\n"
        report3 += "| 테마(업종) | 종목명 | 이격도 |\n| --- | --- | --- |\n"
        for r in sorted(profit_only, key=lambda x: x['sector'])[:40]:
            report3 += f"| {r['sector']} | {r['name']} | {r['disparity']}% |\n"
        send_discord_message(report3)

        # --- 4번 메시지: 3번 기업들 수급 정리표 ---
        # 3번 리스트 기업들의 수급 데이터 일괄 수집
        today = datetime.now().strftime("%Y%m%d")
        purchase_df = stock.get_market_net_purchases_of_equities_by_ticker(today, today, "ALL")
        
        report4 = "### 💰 4. 3번 기업들 당일 수급 현황 (기관/외인/연기금)\n"
        report4 += "| 종목명 | 기관 | 외국인 | 연기금 |\n| --- | --- | --- | --- |\n"
        
        # 수급 데이터 매칭
        for r in profit_only[:40]:
            code = r['code']
            inst = purchase_df.loc[code, '기관합계'] if code in purchase_df.index else 0
            fore = purchase_df.loc[code, '외국인합계'] if code in purchase_df.index else 0
            pension = purchase_df.loc[code, '연기금등'] if code in purchase_df.index else 0
            
            report4 += f"| {r['name']} | {inst:,} | {fore:,} | {pension:,} |\n"
        
        send_discord_message(report4)

    except Exception as e:
        print(f"❌ 에러 발생: {e}")

if __name__ == "__main__":
    main()
