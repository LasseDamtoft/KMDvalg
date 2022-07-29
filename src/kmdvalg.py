import numpy as np
import pandas as pd
from bs4 import BeautifulSoup

from req_trys import req_trys
from scraper import KMDScraper


class KMDValg:
    def __init__(
        self,
        logger,
        processes=12,
        region_url='R84712',
        base_url_rv='https://www.kmdvalg.dk/rv/2021/',
        base_url_ft='https://www.kmdvalg.dk/fv/2019/'
    ):
        self.logger = logger
        self.base_url_rv = base_url_rv
        self.base_url_ft = base_url_ft
        self.region_url = region_url
        self.processes = processes

    def scrape_kv_to_csv(self, path='personlige_stemmer_pr_stemmested.csv'):
        np.arange(5)
        kmd_scraper = KMDScraper(self.logger, self.processes, self.base_url_rv)
        letters = kmd_scraper.get_letters(f'{self.base_url_rv}{self.region_url}.htm', self.region_url)
        local_areas, hierachy = self.get_areas_kv(kmd_scraper)
        local_areas['tmp'] = letters['tmp'] = 1
        area_parties = local_areas.merge(letters, on='tmp').drop('tmp', axis=1)
        votes = kmd_scraper.fetch_candidate_votes(area_parties)
        votes_hierachy = votes.merge(hierachy, on='url')
        votes_csv_ready = votes_hierachy[
            ['kommune', 'stemmested', 'parti', 'url_letter', 'name', 'personal_votes']
        ].copy()
        votes_csv_ready.columns = [
            'kommune', 'stemmested', 'parti', 'partibogstav', 'kandidat_navn', 'personlige_stemmer'
        ]
        votes_csv_ready.to_csv(f'{self.region_url}_2{path}')
        return votes_csv_ready

    def scrape_ft_to_csv(self, path='personlige_stemmer_pr_stemmested.csv'):
        kmd_scraper = KMDScraper(self.logger, self.processes, self.base_url_ft)
        letters = kmd_scraper.get_letters(f'{self.base_url_ft}F1003.htm', 'F1003')
        local_areas, hierachy = self.get_areas_ft(kmd_scraper)
        local_areas['tmp'] = letters['tmp'] = 1
        area_parties = local_areas.merge(letters, on='tmp').drop('tmp', axis=1)
        votes = kmd_scraper.fetch_candidate_votes(area_parties)
        votes_hierachy = votes.merge(hierachy, on='url')
        votes_csv_ready = votes_hierachy[
            ['stemmested', 'kommune', 'kreds', 'storkreds', 'parti', 'url_letter', 'name', 'personal_votes']
        ].copy()
        votes_csv_ready.columns = [
            'stemmested', 'kommune', 'kreds', 'storkreds', 'parti', 'partibogstav', 'kandidat_navn',
            'personlige_stemmer'
        ]
        votes_csv_ready.to_csv(f'{path}')
        return votes_csv_ready

    def get_areas_kv(self, kmd_scraper):
        search_df = pd.DataFrame(
            {
                'area': ['all'],
                'url': [self.region_url]
            }
        )
        search_df_res = kmd_scraper.area_wrap(search_df)

        hierachy = search_df_res.loc[:, search_df_res.columns.str.contains('parent')].copy()
        hierachy = hierachy.iloc[:, 0:2]
        hierachy.columns = ['stemmested', 'kommune']
        hierachy['url'] = search_df_res.iloc[:, 1:2]
        hierachy = hierachy.fillna({'stemmested': hierachy.kommune}).drop_duplicates()
        bot_res = search_df_res.iloc[:, :2].copy()
        bot_res.columns = ['area', 'url']
        return bot_res, hierachy

    def get_areas_ft(self, kmd_scraper):
        resp = req_trys('https://www.kmdvalg.dk/fv/2019/KMDValgFV.html', self.logger)
        soup = BeautifulSoup(resp.content, "html.parser")
        all_results = soup.find(class_='col-sm-12 content-block kmd-list-items')
        storkredse = all_results.find_all(class_='list-group-item')
        storkredse_df = pd.DataFrame(
            {
                f'area': [st.text.strip() for st in storkredse],
                'url': [
                    st.get('href').replace('.htm', '').strip() if isinstance(
                        st.get('href'), str
                    ) else st.get('href') for st in storkredse
                ]
            }
        )
        storkredse_df['storkreds'] = np.where(storkredse_df.url.isna(), storkredse_df.area, np.nan)
        storkredse_df['storkreds'] = storkredse_df['storkreds'].fillna(method='ffill')
        kredse_df = storkredse_df[~storkredse_df.url.isna()]
        search_df_res = kmd_scraper.area_wrap(kredse_df)

        hierachy = search_df_res.loc[:, search_df_res.columns.str.contains(
            'parent'
        ) | search_df_res.columns.str.contains('storkreds')
        ].copy().sort_values(['storkreds', 'parent'])
        hierachy.columns = ['stemmested', 'kommune', 'kreds', 'storkreds']
        hierachy['url'] = search_df_res.iloc[:, 1:2]
        bot_res = search_df_res.iloc[:, :2].copy()
        bot_res.columns = ['area', 'url']
        return bot_res, hierachy
