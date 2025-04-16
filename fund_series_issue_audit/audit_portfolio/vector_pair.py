from ..basis import compute_normalized_inner_product
import numpy as np

class VectorPair:
    def __init__(self, pv_i, pv_j):
        self.pv_i = pv_i
        self.pv_j = pv_j
        self.comparison = None
        self.basis = None
        self.vector_i = None
        self.vector_j = None
        self.coeffs_i = None
        self.coeffs_j = None
        self.inner_product = None
        self._load_pipeline()

    def get_comparison(self, option_delta=True):
        if self.comparison is None:
            comparison = self.pv_i.portfolio.merge(self.pv_j.portfolio, how='outer', left_index=True, right_index=True, suffixes=(f'_{self.pv_i.fund_code}', f'_{self.pv_j.fund_code}'))
            if self.pv_i.fund_code == self.pv_j.fund_code:
                comparison = self.pv_i.portfolio.merge(self.pv_j.portfolio, how='outer', left_index=True, right_index=True, suffixes=(f'_{self.pv_i.fund_code}({self.pv_i.date_ref})', f'_{self.pv_j.fund_code}({self.pv_j.date_ref})'))            
            if option_delta:
                comparison['delta'] = comparison.iloc[:,-3] - comparison.iloc[:,-1]
            comparison = comparison.fillna('-')
            self.comparison = comparison
        return self.comparison

    def get_basis(self):
        if self.basis is None:
            basis = np.array(self.get_comparison().index)
            self.basis = basis
        return self.basis

    def get_vectors(self):
        if self.coeffs_i is None and self.coeffs_j is None:
            comparison = self.get_comparison()
            # 첫 번째 컬럼과 세 번째 컬럼을 추출 (delta 컬럼이 추가되었을 경우 고려)
            col_i = 0 if 'delta' in comparison.columns else 0
            col_j = 2 if 'delta' in comparison.columns else 1
            
            # '-' 문자를 0으로 변환하고 숫자형으로 변환
            self.vector_i = comparison.iloc[:, col_i:col_i+1].replace('-', 0).astype(float)
            self.vector_j = comparison.iloc[:, col_j:col_j+1].replace('-', 0).astype(float)
            
            # 벡터 추출
            self.coeffs_i = np.array(self.vector_i.iloc[:, 0], dtype=float)
            self.coeffs_j = np.array(self.vector_j.iloc[:, 0], dtype=float)
        return self.coeffs_i, self.coeffs_j

    def get_inner_product(self):
        if self.inner_product is None:
            self.inner_product = compute_normalized_inner_product(self.coeffs_i, self.coeffs_j)
        return self.inner_product
    
    def _load_pipeline(self):
        try:
            self.get_comparison()
            self.get_basis()
            self.get_vectors()
            self.get_inner_product()
            return True
        except Exception as e:
            print(f'VectorPair _load_pipeline error: {e}')
            return False

