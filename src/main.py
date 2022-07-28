import pandas as pd

from kmdvalgft import KMDValgFT
from kmdvalgkvrv import KMDValgKVRV
from loginit import LogInitializer

if __name__ == '__main__':
    logger = LogInitializer().logger
    kmd_scraper = KMDValgFT(logger, processes=12)
    kmd_scraper.scrape_to_csv()
    kmd_scraper = KMDValgKVRV(logger, processes=12, region_url='R84712')
    kmd_scraper = KMDValgKVRV(logger, processes=12, region_url='K84712820', base_url='https://kmdvalg.dk/KV/2021/')
