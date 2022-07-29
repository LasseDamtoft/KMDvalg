from kmdvalg import KMDValg
from loginit import LogInitializer

if __name__ == '__main__':
    logger = LogInitializer().logger
    kmd_scraper = KMDValg(logger, processes=12)
    kmd_scraper.scrape_ft_to_csv('personlige_stemmer_pr_stemmested2.csv')
