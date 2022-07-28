import numpy as np
import pandas as pd
from bs4 import BeautifulSoup

from req_trys import req_trys
from scraper import KMDScraper


class KMDValgFT:
    def __init__(self, logger, processes=12, base_url='https://www.kmdvalg.dk/fv/2019/'):
        self.logger = logger
        self.base_url = base_url
        self.processes = processes
        self.kmd_scraper = KMDScraper(logger, processes, base_url)

    def scrape_to_csv(self, path='personlige_stemmer_pr_stemmested.csv'):
        np.arange(5)
        letters = self.kmd_scraper.get_letters('https://www.kmdvalg.dk/fv/2019/F1003.htm', 'F1003')
        local_areas, hierachy = self.get_areas()
        local_areas['tmp'] = letters['tmp'] = 1
        area_parties = local_areas.merge(letters, on='tmp').drop('tmp', axis=1)
        votes = self.kmd_scraper.fetch_candidate_votes(area_parties)
        votes_hierachy = votes.merge(hierachy, left_on='area', right_on='stemmested')
        votes_csv_ready = votes_hierachy[
            ['kommune', 'stemmested', 'parti', 'url_letter', 'name', 'personal_votes']
        ].copy()
        votes_csv_ready.columns = [
            'kommune', 'stemmested', 'parti', 'partibogstav', 'kandidat_navn', 'personlige_stemmer'
        ]
        votes_csv_ready.to_csv(f'{path}')
        return votes_csv_ready

    def get_areas(self):
        resp = req_trys('https://www.kmdvalg.dk/fv/2019/KMDValgFV.html', self.logger)
        soup = BeautifulSoup(resp.content, "html.parser")
        all_results = soup.find(class_='col-sm-12 content-block kmd-list-items')
        storkredse = all_results.find_all(class_='list-group-item')
        storkredse_df = pd.DataFrame(
            {
                f'area': [st.text for st in storkredse],
                'url': [
                    st.get('href').replace('.htm', '') if isinstance(st.get('href'), str) else st.get('href') for st
                    in storkredse
                ]
            }
        )
        storkredse_df['storkreds'] = np.where(storkredse_df.url.isna(), storkredse_df.area, np.nan)
        storkredse_df['storkreds'] = storkredse_df['storkreds'].fillna(method='ffill')
        kredse_df = storkredse_df[~storkredse_df.url.isna()]
        search_df_res = self.kmd_scraper.area_wrap(kredse_df)

        hierachy = search_df_res.loc[:, search_df_res.columns.str.contains(
            'parent'
        ) | search_df_res.columns.str.contains('storkreds')
        ].copy().sort_values(['storkreds', 'parent'])
        hierachy.columns = ['stemmested', 'kommune', 'kreds', 'storkreds']
        na_mask = hierachy.stemmested.isna()
        hierachy = hierachy.fillna({'stemmested': hierachy.kommune})
        hierachy.loc[na_mask, 'kommune'] = hierachy.loc[na_mask, 'kreds']
        temp = search_df_res.loc[:, search_df_res.columns.str.contains('url')].T
        search_df_res.loc[:, search_df_res.columns.str.contains('url')] = temp.fillna(method='bfill').T
        temp = search_df_res.loc[:, search_df_res.columns.str.contains('area')].T
        search_df_res.loc[:, search_df_res.columns.str.contains('area')] = temp.fillna(method='bfill').T
        search_df_res = search_df_res[(search_df_res.url_bot1 == search_df_res.url_bot0)].reset_index(drop=True).copy()
        bot_res = search_df_res.iloc[:, :2].copy()
        bot_res.columns = ['area', 'url']
        return bot_res, hierachy
