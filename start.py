import FinanceDataReader as fdr
from pykrx import stock
import requests
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
import time

IGYEOK_WEBHOOK_URL = "https://discord.com/api/webhooks/1463876197027942514/N9wyH6wL3HKmMSFxNjL1nSbjuoc6q0cZ_nNi9iPILmDecmiIzjU9gDAgGKpUV0A_fSzl"

def send_discord_message(content):
    if not content or len(content.strip()) < 10: return
    try:
        if len(content) > 1900:
            chunks = [content[i:i+1900] for i in range(0, len(content), 1900)]
            for chunk in chunks:
                requests.post(IGYEOK_WEBHOOK_URL, json={'content': chunk})
                time.sleep(1)
        else:
            requests.post(IGYEOK_WEBHOOK_URL, json={'content': content})
    except Exception as e:
        print(f"전송 에러: {e}")

def get_detailed_info(code):
    """네이버 금융에서 업종 및 영업이익 직접 확인"""
    url = f"https://finance.naver.com/item/main.naver?code={code}"
    try:
        res = requests.get(url, timeout=5)
        soup = BeautifulSoup(res.text, 'lxml')
        
        sector = "기타"
        h4_tags = soup.find_all('h4')
        for h4 in h4_tags:
            if '업종명' in h4.text:
                sector = h4.find_next('em').text.strip()
                break
        
        is_profit = False
        table = soup.find('table', {'class': 'tb_type1 tb_num'})
        if table:
            profit_row = table.find('th', string='영업이익')
            if profit_row:
                target_td = profit_row.find_next('td')
                if target_td:
                    val = target_td.text.replace(',', '').strip()
                    if val and val != '-' and int(val) > 0:
                        is_profit = True
        return sector, is_profit
    except:
        return "기타", False

def main():
    print("🚀 [분석 시작] 계단식 이격도 분석 (90% 우선)")
    try:
        # 1. 대상 종목 확보 (코스피 500, 코스닥 500)
        df_krx = fdr.StockListing('KRX')
        df_kospi = df_krx[df_krx['Market']=='KOSPI'].head(500)
        df_kosdaq = df_krx[df_krx['Market']=='KOSDAQ'].head(500)
        target_codes = pd.concat([df_kospi, df_kosdaq])

        all_results = []
        today = datetime.now().strftime("%Y%m%d")
        purchase_df = stock.get_market_net_purchases_of_equities_by_ticker(today, today, "ALL")

        print(f"📡 총 {len(target_codes)}개 종목 분석 중...")

        for idx, row in target_codes.iterrows():
            code, name = row['Code'], row['Name']
            try:
                df = fdr.DataReader(code).tail(30)
                if len(df) < 20: continue
                curr = df['Close'].iloc[-1]
                ma20 = df['Close'].rolling(window=20).mean().iloc[-1]
                disparity = round((curr / ma20) * 100, 1)

                # 우선 모든 분석 데이터를 수집 (나중에 계단식으로 필터링)
                if disparity <= 95.0:
                    sector, is_profit = get_detailed_info(code)
                    inst = purchase_df.loc[code, '기관합계'] if code in purchase_df.index else 0
                    fore = purchase_df.loc[code, '외국인합계'] if code in purchase_df.index else 0
                    pen = purchase_df.loc[code, '연기금등'] if code in purchase_df.index else 0
                    
                    all_results.append({
                        'name': name, 'code': code, 'disparity': disparity, 
                        'sector': sector, 'is_profit': is_profit,
                        'inst': inst, 'fore': fore, 'pen': pen
                    })
            except: continue

        # --- 1단계: 계단식 필터링 로직 ---
        # 90% 이하가 있는지 먼저 확인
        final_list = [r for r in all_results if r['disparity'] <= 90.0]
        filter_status = "90% 이하 (초과낙폭)"

        # 90% 이하가 하나도 없으면 95%까지 확장
        if not final_list:
            final_list = [r for r in all_results if r['disparity'] <= 95.0]
            filter_status = "95% 이하 (일반낙폭)"

        if not final_list:
            send_discord_message("🔍 조건에 맞는 종목이 없습니다.")
            return

        # --- 리포트 전송 ---
        # 1. 이격도 분석 결과
        rep1 = f"### 📊 1. 이격도 분석 결과 ({filter_status})\n"
        for r in sorted(final_list, key=lambda x: x['disparity'])[:50]:
            rep1 += f"· **{r['name']}({r['code']})**: {r['disparity']}%\n"
        send_discord_message(rep1)

        # 2. 테마분류표
        rep2 = "### 📋 2. 1번 기업들 테마분류표\n| 테마(업종) | 종목명 | 이격도 |\n| --- | --- | --- |\n"
        for r in sorted(final_list, key=lambda x: x['sector'])[:40]:
            rep2 += f"| {r['sector']} | {r['name']} | {r['disparity']}% |\n"
        send_discord_message(rep2)

        # 3. 흑자기업 필터링
        profit_only = [r for r in final_list if r['is_profit']]
        rep3 = "### 📉 3. 흑자기업 필터링 (적자 제외)\n| 테마(업종) | 종목명 | 이격도 |\n| --- | --- | --- |\n"
        if not profit_only:
            rep3 += "| - | 해당되는 흑자 기업 없음 | - |\n"
        else:
            for r in sorted(profit_only, key=lambda x: x['sector'])[:40]:
                rep3 += f"| {r['sector']} | {r['name']} | {r['disparity']}% |\n"
        send_discord_message(rep3)

        # 4. 당일 수급 현황 (3번 리스트 기준)
        rep4 = "### 💰 4. 3번 기업들 당일 수급 현황 (기관/외인/연기금)\n| 종목명 | 기관 | 외국인 | 연기금 |\n| --- | --- | --- | --- |\n"
        source = profit_only if profit_only else final_list
        for r in source[:40]:
            rep4 += f"| {r['name']} | {r['inst']:,} | {r['fore']:,} | {r['pen']:,} |\n"
        send_discord_message(rep4)

    except Exception as e:
        print(f"❌ 에러: {e}")

if __name__ == "__main__":
    main()
