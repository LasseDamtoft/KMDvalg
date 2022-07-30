from kmdvalg import KMDValg
from loginit import LogInitializer

if __name__ == '__main__':
    logger = LogInitializer().logger
    kmd_scraper = KMDValg(logger, processes=12)
    res_kv = kmd_scraper.scrape_general('kv')
    # res_kv = pd.read_csv('personlige_stemmer_pr_stemmested_KV.csv')
    res_rv = kmd_scraper.scrape_general('rv')
    res_ft = kmd_scraper.scrape_general('ft')
    md_res = res_rv[res_rv.kandidat_navn == 'Mads Duedahl, Aalborg'].personlige_stemmer.sum()
    pbl_res = res_kv[res_kv.kandidat_navn == 'Per Bach Laursen'].personlige_stemmer.sum()
    mf_res = res_ft[res_ft.kandidat_navn == 'Jakob Ellemann-Jensen'].personlige_stemmer.sum()

    logger.info(f'Mads Duedahls votecount is fetched correct: {md_res == 22007}')
    logger.info(f'Per Bachs votecount is fetched correct: {pbl_res == 2965}')
    logger.info(f'Jakob Ellemann-Jensen votecount is fetched correct: {mf_res == 19388}')

    logger.info(f'all done')
