import multiprocessing
import time

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup


class KMDValg:
    def __init__(self, logger, processes=12, region_url='R84712'):
        self.logger = logger
        self.base_url = 'https://www.kmdvalg.dk/rv/2021/'
        self.region_url = region_url
        self.processes = processes

    def scrape_to_csv(self, path='personlige_stemmer_pr_stemmested.csv'):
        np.arange(5)
        letters = self.get_letters()
        local_areas, hierachy = self.get_areas()
        local_areas['tmp'] = letters['tmp'] = 1
        area_parties = local_areas.merge(letters, on='tmp').drop('tmp', axis=1)
        votes = self.fetch_candidate_votes(area_parties)
        votes_hierachy = votes.merge(hierachy, left_on='area', right_on='stemmested')
        votes_csv_ready = votes_hierachy[
            ['kommune', 'stemmested', 'parti', 'url_letter', 'name', 'personal_votes']
        ].copy()
        votes_csv_ready.columns = [
            'kommune', 'stemmested', 'parti', 'partibogstav', 'kandidat_navn', 'personlige_stemmer'
        ]
        votes_csv_ready.to_csv(path)
        return votes_csv_ready

    def fetch_candidate_votes(self, area_parties):
        self.logger.info(f'Fetching personal votes')
        with multiprocessing.Pool(self.processes) as pool:
            votes_loop = pool.starmap(
                self.fetch_personal_votes,
                [
                    (
                        area,
                        self.base_url + area.url + area.url_letter + '.htm',
                        self.logger
                    ) for _, area in area_parties.iterrows()
                ]
            )
        self.logger.info(f'Fetching personal votes done')
        votes = pd.concat(votes_loop)
        votes_merged = area_parties.merge(votes, on=['area', 'url_letter'], how='outer')
        return votes_merged

    def get_areas(self):
        search_df = pd.DataFrame(
            {
                'area': ['all'],
                'url': [self.region_url]
            }
        )
        res = self.areas(search_df)
        search_df_res = res.merge(
            search_df,
            left_on='parent',
            right_on='area',
            suffixes=('_bot', '')
        )
        hierachy = search_df_res.loc[:, search_df_res.columns.str.contains('parent')].copy()
        assert hierachy.shape[1] == 3
        hierachy2 = hierachy.iloc[:, 0:2]
        hierachy2.columns = ['stemmested', 'kommune']
        bot_res = search_df_res.iloc[:, :2].copy()
        bot_res.columns = ['area', 'url']
        return bot_res, hierachy2

    def get_letters(self):
        self.logger.info(f'Fetching party letters')
        resp = req_trys(f'{self.base_url}{self.region_url}.htm', self.logger)
        soup = BeautifulSoup(resp.content, "html.parser")
        results = soup.find("div", class_='col-xs-12 col-sm-6 col-md-8 content-block kmd-parti-list')
        parti_bogstaver = pd.DataFrame(
            {
                'parti': [link.text for link in results.find_all("a")],
                'url_letter': [
                    link['href'].lower().replace(
                        self.region_url.lower(), ''
                    ).replace('.htm', '') for link in results.find_all("a")
                ]
            }
        )
        return parti_bogstaver

    @staticmethod
    def fetch_area(row, url, logger):
        resp = req_trys(url, logger)
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
        return search_df_loop

    @staticmethod
    def fetch_personal_votes(area, url, logger):
        resp = req_trys(url, logger)
        soup = BeautifulSoup(resp.content, "html.parser")
        votes_loop = pd.DataFrame(
            {
                'area': area.area,
                'url_letter': area.url_letter,
                'name': [
                    name.text for name in soup.find_all(
                        "div",
                        class_='table-like-cell col-xs-7 col-sm-6 col-md-6 col-lg-8'
                    )
                ],
                'personal_votes': [
                    personal_votes.text for personal_votes in soup.find_all(
                        'div',
                        class_='table-like-cell col-xs-5 col-sm-6 col-md-6 col-lg-4 text-right roboto-bold'
                    )
                ]
            }
        )
        return votes_loop

    def areas(self, search_df, level=0):
        self.logger.info(f'Fetching areas level {level}')
        with multiprocessing.Pool(self.processes) as pool:
            search_df_loop = pool.starmap(
                self.fetch_area,
                [
                    (
                        row,
                        self.base_url + row.url + '.htm',
                        self.logger
                    ) for _, row in search_df.iterrows()
                ]
            )
        search_df_res = pd.concat(search_df_loop)
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


def req_trys(url, logger, method='get'):
    trys = 0
    while trys < 5:
        try:
            with requests.Session() as s:
                if method == 'get':
                    resp = s.get(url)
                elif method == 'post':
                    resp = s.post(url)
            return resp
        except:
            timer = np.random.uniform(0, 4, 1)[0]
            logger.warning(f'Failed fetching from {url}, trying again in {np.round(timer, 2)} seconds')
            time.sleep(timer)
            continue
    logger.error(f'Failed fetching from {url} 5 times')
