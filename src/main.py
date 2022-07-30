import pandas as pd

from kmdvalg import KMDValg
from loginit import LogInitializer

if __name__ == '__main__':
    logger = LogInitializer().logger
    kmd_scraper = KMDValg(logger, processes=12)
    try:
        res_kv = pd.read_csv('personlige_stemmer_pr_stemmested_KV.csv')
    except:
        res_kv = kmd_scraper.scrape_general('kv')
    try:
        res_rv = pd.read_csv('personlige_stemmer_pr_stemmested_RV.csv')
    except:
        res_rv = kmd_scraper.scrape_general('rv')
    try:
        res_ft = pd.read_csv('personlige_stemmer_pr_stemmested_FT.csv')
    except:
        res_ft = kmd_scraper.scrape_general('ft')
    md_res = res_rv[res_rv.kandidat_navn == 'Mads Duedahl, Aalborg']
    pbl_res = res_kv[res_kv.kandidat_navn == 'Per Bach Laursen']
    sl_res = res_rv[res_rv.kandidat_navn == 'Stephanie Lose, Esbjerg']
    jej_res = res_ft[res_ft.kandidat_navn == 'Jakob Ellemann-Jensen']
    ktd_res = res_ft[res_ft.kandidat_navn == 'Kristian Thulesen Dahl']

    logger.info(f'Mads Duedahls votecount is fetched correct: {md_res.personlige_stemmer.sum() == 22007}')
    logger.info(f'Per Bachs votecount is fetched correct: {pbl_res.personlige_stemmer.sum() == 2965}')
    logger.info(f'Jakob Ellemann-Jensen votecount is fetched correct: {jej_res.personlige_stemmer.sum() == 19388}')
    logger.info(f'Kristian Thulesen Dahl votecount is fetched correct: {ktd_res.personlige_stemmer.sum() == 23142}')
    logger.info(f'Stephanie Lose votecount is fetched correct: {sl_res.personlige_stemmer.sum() == 147486}')

    logger.info(f'all done')
