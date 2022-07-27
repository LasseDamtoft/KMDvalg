import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup


class KMDValg:
    def __init__(self, logger):
        self.logger = logger
        self.base_url = 'https://www.kmdvalg.dk/rv/2021/'

    def run(self):
        np.arange(5)
        letters = self.get_letters()
        local_areas, hierachy = self.get_areas()
        local_areas['tmp'] = letters['tmp'] = 1
        area_parties = local_areas.merge(letters, on='tmp').drop('tmp', axis=1)
        return area_parties

    def get_areas(self):
        search_df = pd.DataFrame(
            {
                'area': ['all'],
                'url': ['R84712']
            }
        )
        res = self.areas(search_df)
        search_df_res = res.merge(
            search_df,
            left_on='parent',
            right_on='area',
            suffixes=[f'_bot', '']
        )
        hierachy = search_df_res.loc[:, search_df_res.columns.str.contains('parent')].copy()
        bot_res = search_df_res.iloc[:, :2].copy()
        bot_res.columns = ['area', 'url']
        return bot_res, hierachy

    def get_letters(self):
        self.logger.info(f'Getting party letters')
        with requests.Session() as s:
            resp = s.get(f'{self.base_url}R84712.htm')
        soup = BeautifulSoup(resp.content, "html.parser")
        results = soup.find("div", class_='col-xs-12 col-sm-6 col-md-8 content-block kmd-parti-list')
        parti_bogstaver = pd.DataFrame(
            {
                'parti': [link.text for link in results.find_all("a")],
                'url_letter': [link['href'].replace('r84712', '').replace('.htm', '') for link in results.find_all("a")]
            }
        )
        return parti_bogstaver

    def areas(self, search_df, level=0):
        search_df_res = pd.DataFrame()
        for _, row in search_df.iterrows():
            with requests.Session() as s:
                resp = s.get(f'{self.base_url}{row.url}.htm')
            self.logger.info(f'Searching {row.area}')
            soup = BeautifulSoup(resp.content, "html.parser")
            results = soup.find(id='vote-areas')
            if results:
                search_df_loop = pd.DataFrame(
                    {
                        f'area': [link.text for link in results.find_all("a")],
                        'url': [link['href'].replace('.htm', '') for link in results.find_all("a")]
                    }
                )
            else:
                search_df_loop = pd.DataFrame(
                    {
                        f'area': [row.area],
                        'url': [row.url.replace('.htm', '')]
                    }
                )
            search_df_loop['parent'] = row.area
            search_df_res = pd.concat([search_df_res, search_df_loop])
        further_search = search_df_res[search_df_res.area != search_df_res.parent].copy()
        if not further_search.empty:
            search_sub = self.areas(further_search, level=level + 1)
            search_df_res = search_sub.merge(
                search_df_res,
                left_on='parent',
                right_on='area',
                suffixes=[f'_bot{level}', '']
            )
        return search_df_res
