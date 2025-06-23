import numpy as np
from shining_pebbles import get_yesterday
from .result_utils import (
    get_df_filtered_series_issue_audit,
    load_series_issue_audit_result,
    get_comparison_of_row_in_df
)

class SeriesIssueAudit:
    def __init__(self, date_ref=None, option_threshold=0.8):
        self.date_ref = date_ref if date_ref else get_yesterday()
        self.option_threshold = option_threshold
        self._generate = None
        self._load = None
        self._concise = None

    @property
    def generate(self):
        print(f'date_ref: {self.date_ref}')
        if self._generate is None:
            self._generate = get_df_filtered_series_issue_audit(date_ref=self.date_ref)
        return self._generate

    @property
    def load(self):
        print(f'date_ref: {self.date_ref}')
        if self._load is None:
            self._load = load_series_issue_audit_result(date_ref=self.date_ref, option_threshold=self.option_threshold, option_concise=False)
        return self._load

    @property
    def concise(self):
        print(f'date_ref: {self.date_ref}')
        if self._concise is None:
            self._concise = load_series_issue_audit_result(date_ref=self.date_ref, option_threshold=self.option_threshold, option_concise=True)
        return self._concise

    def comparison(self, index):
        df = get_comparison_of_row_in_df(self.load, index, date_ref=self.date_ref).copy()
        df['delta'] = df['delta'].map(lambda x: x if x!='-' else np.nan)
        df = df.sort_values(by='delta', ascending=False)
        df['delta'] = df['delta'].fillna('-')
        return df