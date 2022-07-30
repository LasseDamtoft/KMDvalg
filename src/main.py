from kmdvalg import KMDValg
from loginit import LogInitializer

if __name__ == '__main__':
    logger = LogInitializer().logger
    kmd_scraper = KMDValg(logger, processes=12)
    res_rv = kmd_scraper.scrape_general('rv')
    res_kv = kmd_scraper.scrape_general('kv')
    res_ft = kmd_scraper.scrape_general('ft')
    # res_kv = pd.read_csv('personlige_stemmer_pr_stemmested_KV.csv')
    md_res = res_rv[res_rv.kandidat_navn == 'Mads Duedahl, Aalborg'].personlige_stemmer.sum()
    pbl_res = res_kv[res_kv.kandidat_navn == 'Per Bach Laursen'].personlige_stemmer.sum()
    sl_res = res_rv[res_rv.kandidat_navn == 'Stephanie Lose, Esbjerg'].personlige_stemmer.sum()
    jej_res = res_ft[res_ft.kandidat_navn == 'Jakob Ellemann-Jensen'].personlige_stemmer.sum()
    ktd_res = res_ft[res_ft.kandidat_navn == 'Kristian Thulesen Dahl'].personlige_stemmer.sum()

    logger.info(f'Mads Duedahls votecount is fetched correct: {md_res == 22007}')
    logger.info(f'Per Bachs votecount is fetched correct: {pbl_res == 2965}')
    logger.info(f'Jakob Ellemann-Jensen votecount is fetched correct: {jej_res == 19388}')
    logger.info(f'Kristian Thulesen Dahl votecount is fetched correct: {ktd_res == 23142}')
    logger.info(f'Stephanie Lose votecount is fetched correct: {sl_res == 147486}')

    logger.info(f'all done')
