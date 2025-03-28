from .basis import compute_normalized_dot_product
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
        self.dot_product = None
        self._load_pipeline()

    def get_comparison(self):
        if self.comparison is None:
            comparison = self.pv_i.portfolio.merge(self.pv_j.portfolio, how='outer', left_index=True, right_index=True, suffixes=(f'_{self.pv_i.fund_code}', f'_{self.pv_j.fund_code}'))
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
            self.vector_i = comparison.iloc[:, 1:2].fillna(0)
            self.vector_j = comparison.iloc[:, 3:4].fillna(0)
            self.coeffs_i = np.array(self.vector_i.iloc[: ,-1])
            self.coeffs_j = np.array(self.vector_j.iloc[: ,-1])
        return self.coeffs_i, self.coeffs_j

    def get_dot_product(self):
        if self.dot_product is None:
            self.dot_product = compute_normalized_dot_product(self.coeffs_i, self.coeffs_j)
        return self.dot_product
    
    def _load_pipeline(self):
        try:
            self.get_comparison()
            self.get_basis()
            self.get_vectors()
            self.get_dot_product()
            return True
        except Exception as e:
            print(f'_load_pipeline error: {e}')
            return False

