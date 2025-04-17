from fund_series_issue_audit import PortfolioVector
from fund_insight_engine.s3_retriever import get_mapping_fund_names_of_division_01
from shining_pebbles import get_yesterday
from tqdm import tqdm
import pandas as pd

def fetch_portfolio_listed(fund_code, date_ref=None):
    date_ref = date_ref if date_ref else get_yesterday()
    pv = PortfolioVector(fund_code=fund_code, date_ref=date_ref)
    raw = pv.get_raw_portfolio()
    df = raw[raw['종목정보: 자산분류'].str.contains('거래소상장_주식|코스닥상장_주식')]
    COLS_TO_KEEP = ['종목', '종목명', '원화 보유정보: 수량','원화 보유정보: 평가액', '원화 보유정보: 취득액', '원화 보유정보: 평가손익']
    df = df[COLS_TO_KEEP]
    return df

def fetch_portfolio_division_01(date_ref=None):
    date_ref = date_ref if date_ref else get_yesterday()
    dfs = []
    mapping_division_01 = get_mapping_fund_names_of_division_01()
    for fund_code, fund_name in tqdm(mapping_division_01.items()):
        print(fund_code, fund_name)
        try:
            df = fetch_portfolio_listed(fund_code=fund_code, date_ref=date_ref)
            dfs.append(df)
        except Exception as e:
            print(f'PortfolioVector error: {e}')
    portfolio_division_01 = pd.concat(dfs, axis=0)
    portfolio_division_01 = portfolio_division_01.groupby('종목').agg({'종목명': 'first', '원화 보유정보: 수량': 'sum', '원화 보유정보: 평가액': 'sum', '원화 보유정보: 취득액': 'sum', '원화 보유정보: 평가손익': 'sum'})
    portfolio_division_01['손익률'] = (portfolio_division_01['원화 보유정보: 평가액'] / portfolio_division_01['원화 보유정보: 취득액'] -1) * 100
    return portfolio_division_01

def search_equity_info_including_keyword(df, keyword):
    return df[df['종목명'].str.contains(keyword)]