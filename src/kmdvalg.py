import numpy as np
import pandas as pd


class KMDValg:
    def __init__(self, logger):
        self.logger = logger

    def run(self):
        ar = np.arange(5)
        df = pd.DataFrame()
        self.logger.info(f'test')
