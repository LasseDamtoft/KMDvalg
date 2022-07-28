from kmdvalg import KMDValg
from loginit import LogInitializer

if __name__ == '__main__':
    logger = LogInitializer().logger
    kmd_scraper = KMDValg(logger, processes=12, region_url='R84712')
    kmd_scraper.scrape_to_csv()
