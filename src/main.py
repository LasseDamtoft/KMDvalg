from loginit import LogInitializer
from kmdvalg import KMDValg

if __name__ == '__main__':
    logger = LogInitializer().logger
    kmd_scraper = KMDValg(logger)
    kmd_scraper.run()
