from kmdvalg import KMDValg
from loginit import LogInitializer

if __name__ == '__main__':
    logger = LogInitializer().logger
    kmd_scraper = KMDValg(logger, processes=12, region_url='K84712820', base_url='https://kmdvalg.dk/KV/2021/')
    kmd_scraper.scrape_to_csv()
    kmd_scraper = KMDValg(logger, processes=12, region_url='R84712')
