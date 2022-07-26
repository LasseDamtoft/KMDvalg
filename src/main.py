import structlog

from kmdvalg import KMDValg

if __name__ == '__main__':
    logger = structlog.get_logger()
    kmd_scraper = KMDValg(logger)
    kmd_scraper.run()
