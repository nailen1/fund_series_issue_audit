from shining_pebbles import open_df_in_file_folder_by_regex, get_yesterday, get_today
from .general_utils import FUND_CODES_MAIN, MAPPING_FUND_NAMES, MAPPING_INCEPTION_DATES
from .portfolio_vector import PortfolioVector
from .vector_pair import VectorPair
from .date_condition_utils import get_pairs_filtered_by_6_months_inception_condition
from .asset_condition_utils import get_asset_vector_string
from .path_director import FILE_FOLDER
from tqdm import tqdm
import pandas as pd
from shining_pebbles import get_today, get_yesterday, get_date_n_days_ago
from canonical_transformer import map_dataframe_to_csv_including_korean


# def get_inner_products_of_fund_series_issue():
#     # vps = []
#     inner_products = []
#     for fund_code_i in tqdm(FUND_CODES_MAIN):
#         pv_i = PortfolioVector(fund_code=fund_code_i)
#         for fund_code_j in tqdm(FUND_CODES_MAIN):
#             pv_j = PortfolioVector(fund_code=fund_code_j)
#             vp = VectorPair(pv_i=pv_i, pv_j=pv_j)
#             inner_product = vp.inner_product
#             dct = {'fund_code_i': fund_code_i, 'fund_code_j': fund_code_j, 'inner_product': inner_product}
#             # vps.append(vp)
#             inner_products.append(dct)
#     return inner_products

def get_inner_products_of_fund_series_issue():
    data = []
    pairs = get_pairs_filtered_by_6_months_inception_condition()
    for i, j in tqdm(pairs):
        pv_i = PortfolioVector(fund_code=i)
        pv_j = PortfolioVector(fund_code=j)
        vp = VectorPair(pv_i, pv_j)
        dct = {'fund_code_i': i, 'fund_code_j': j, 'inner_product': vp.inner_product}
        data.append(dct)
    return data

def save_results_of_fund_series_issue(option_save=True, option_asset_validity=True):
    inner_products = get_inner_products_of_fund_series_issue()
    results = pd.DataFrame(inner_products).sort_values(by='inner_product', ascending=False)
    results = results[results['inner_product']<1.0]
    results['fund_name_i'] = results['fund_code_i'].map(MAPPING_FUND_NAMES)
    results['fund_name_j'] = results['fund_code_j'].map(MAPPING_FUND_NAMES)
    results['inception_date_i'] = results['fund_code_i'].map(MAPPING_INCEPTION_DATES)
    results['inception_date_j'] = results['fund_code_j'].map(MAPPING_INCEPTION_DATES)
    results['asset_vector_i'] = results['fund_code_i'].apply(lambda x: get_asset_vector_string(fund_code=x))
    results['asset_vector_j'] = results['fund_code_j'].apply(lambda x: get_asset_vector_string(fund_code=x))
    results['asset_validity'] = results['asset_vector_i'] == results['asset_vector_j']
    if option_asset_validity:
        results = results[results['asset_validity']==True].reset_index(drop=True)
    if option_save:
        map_dataframe_to_csv_including_korean(results.reset_index(), file_folder=FILE_FOLDER['result'], file_name=f'dataset-series_issue-at{get_yesterday().replace("-", "")}-save{get_today().replace("-", "")}.csv')
    return results

def load_result_series_issue_audit(date_ref=None, option_threshold=True):
    regex = f'dataset-series_issue-at{date_ref.replace("-", "")}' if date_ref else 'dataset-series_issue-at'
    df = open_df_in_file_folder_by_regex(file_folder=FILE_FOLDER['result'], regex=regex)
    if option_threshold:
        df = df[df['inner_product']>=0.8]
    return df

# def load_filtered_result_series_issue_audit(date_ref=None, option_inner_product_threshold=True):
#     regex = f'dataset-series_issue_filtered-at{date_ref.replace("-", "")}' if date_ref else 'dataset-series_issue_filtered-at'
#     results = open_df_in_file_folder_by_regex(file_folder=FILE_FOLDER['result'], regex=regex)
#     if option_inner_product_threshold:
#         results = results[results['inner_product']>=0.8]
#     return results

def extract_pair_fund_codes_of_row_in_df(df, index_row):
    srs = df.iloc[index_row]
    pair = srs['fund_code_i'], srs['fund_code_j']
    return pair
    
def get_vp_of_row_in_df(df, index_row, date_ref=None):
    date_ref = date_ref if date_ref else get_date_n_days_ago(get_yesterday(),1)
    pair_codes = extract_pair_fund_codes_of_row_in_df(df, index_row)
    pv_i = PortfolioVector(fund_code=pair_codes[0], date_ref=date_ref)
    pv_j = PortfolioVector(fund_code=pair_codes[1], date_ref=date_ref)
    vp = VectorPair(pv_i=pv_i, pv_j=pv_j)
    return vp

def get_comparison_of_row_in_df(df, index_row, date_ref=None, option_delta=True):
    date_ref = date_ref if date_ref else get_date_n_days_ago(get_yesterday(),1)
    vp = get_vp_of_row_in_df(df, index_row, date_ref=date_ref)
    comparison = vp.comparison
    if option_delta:
        comparison['delta'] = comparison.iloc[:,-3] - comparison.iloc[:,-1]
    comparison = comparison.fillna('-')
    print(f'Calculated inner product <pv_i|pv_j> == {vp.inner_product}')
    return comparison

# def filter_df_by_inception_date_condition(df, option_save=True):
#     df['days_since_inception_i'] = df['inception_date_i'].apply(lambda x: (pd.Timestamp(get_today()) - pd.Timestamp(x)).days)
#     df['days_since_inception_j'] = df['inception_date_j'].apply(lambda x: (pd.Timestamp(get_today()) - pd.Timestamp(x)).days)
#     df['condition_i: < 6months'] = df['days_since_inception_i'].apply(lambda x: True if x <= 180 else False)
#     df['condition_j: < 6months'] = df['days_since_inception_j'].apply(lambda x: True if x <= 180 else False)
#     df = df[~df.isin([False]).any(axis=1)].reset_index(drop=True)
#     df = df.iloc[:, :7]
#     if option_save:
#         map_dataframe_to_csv_including_korean(df.reset_index(drop=True), file_folder=FILE_FOLDER['result'], file_name=f'dataset-series_issue_filtered-at{get_today().replace("-", "")}-save{get_today().replace("-", "")}.csv')
#     return df

def load_automated_series_issue_audit_result(date_ref=None):
    regex = f'dataset-result_automated_series_issue_audit-at{date_ref.replace("-", "")}' if date_ref else 'dataset-result_automated_series_issue_audit-at'
    df = open_df_in_file_folder_by_regex(file_folder=FILE_FOLDER['result'], regex=regex)
    df = df.reset_index()
    return df