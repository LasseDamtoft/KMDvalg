import numpy as np
import pandas as pd

from scraper import KMDScraper


class KMDValgKVRV:
    def __init__(self, logger, processes=12, region_url='R84712', base_url='https://www.kmdvalg.dk/rv/2021/'):
        self.logger = logger
        self.base_url = base_url
        self.region_url = region_url
        self.processes = processes
        self.kmd_scraper = KMDScraper(logger, processes, base_url)

    def scrape_to_csv(self, path='personlige_stemmer_pr_stemmested.csv'):
        np.arange(5)
        letters = self.kmd_scraper.get_letters(f'{self.base_url}{self.region_url}.htm', self.region_url)
        local_areas, hierachy = self.get_areas()
        local_areas['tmp'] = letters['tmp'] = 1
        area_parties = local_areas.merge(letters, on='tmp').drop('tmp', axis=1)
        votes = self.kmd_scraper.fetch_candidate_votes(area_parties)
        votes_hierachy = votes.merge(hierachy, on='url')
        votes_csv_ready = votes_hierachy[
            ['kommune', 'stemmested', 'parti', 'url_letter', 'name', 'personal_votes']
        ].copy()
        votes_csv_ready.columns = [
            'kommune', 'stemmested', 'parti', 'partibogstav', 'kandidat_navn', 'personlige_stemmer'
        ]
        votes_csv_ready.to_csv(f'{self.region_url}_2{path}')
        return votes_csv_ready

    def get_areas(self):
        search_df = pd.DataFrame(
            {
                'area': ['all'],
                'url': [self.region_url]
            }
        )
        search_df_res = self.kmd_scraper.area_wrap(search_df)

        hierachy = search_df_res.loc[:, search_df_res.columns.str.contains('parent')].copy()
        hierachy = hierachy.iloc[:, 0:2]
        hierachy.columns = ['stemmested', 'kommune']
        hierachy['url'] = search_df_res.iloc[:, 1:2]
        hierachy = hierachy.fillna({'stemmested': hierachy.kommune}).drop_duplicates()
        bot_res = search_df_res.iloc[:, :2].copy()
        bot_res.columns = ['area', 'url']
        return bot_res, hierachy
