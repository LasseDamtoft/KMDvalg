import multiprocessing
from functools import partial

import pandas as pd
import tqdm
from bs4 import BeautifulSoup

from req_trys import req_trys


class KMDScraper:
    def __init__(self, logger, processes, base_url):
        self.logger = logger
        self.processes = processes
        self.base_url = base_url

    @staticmethod
    def fetch_area(args):
        row, url, logger = args[0], args[1], args[2]
        resp = req_trys(url, logger)
        soup = BeautifulSoup(resp.content, "html.parser")
        results = soup.find(id='vote-areas')
        if results:
            search_df_loop = pd.DataFrame(
                {
                    f'area': [link.text.strip() for link in results.find_all("a")],
                    'url': [link['href'].replace('.htm', '').strip() for link in results.find_all("a")]
                }
            )
        else:
            search_df_loop = pd.DataFrame(
                {
                    f'area': [row.area.strip()],
                    'url': [row.url.replace('.htm', '').strip()]
                }
            )
        search_df_loop['parent'] = row.area
        search_df_loop['parent_url'] = row.url
        return search_df_loop

    @staticmethod
    def fetch_personal_votes(args):
        area, url, logger = args[0], args[1], args[2]
        resp = req_trys(url, logger)
        soup = BeautifulSoup(resp.content, "html.parser")
        votes_loop = pd.DataFrame(
            {
                'url_letter': area.url_letter,
                'url': area.url,
                'name': [
                    name.text.strip() for name in soup.find_all(
                        "div",
                        class_='table-like-cell col-xs-7 col-sm-6 col-md-6 col-lg-8'
                    )
                ],
                'personal_votes': [
                    personal_votes.text.strip() for personal_votes in soup.find_all(
                        'div',
                        class_='table-like-cell col-xs-5 col-sm-6 col-md-6 col-lg-4 text-right roboto-bold'
                    )
                ]
            }
        )
        votes_loop.personal_votes = pd.to_numeric(votes_loop.personal_votes.str.replace('.', '', regex=False))
        return votes_loop

    def areas(self, search_df, level=0):
        self.logger.info(f'Fetching areas level {level}')
        search_df_loop = []
        with multiprocessing.Pool(self.processes) as pool:
            for res in tqdm.tqdm(
                pool.imap(
                    partial(self.fetch_area),
                    [
                        (
                            row,
                            self.base_url + row.url + '.htm',
                            self.logger
                        ) for _, row in search_df.iterrows()
                    ]
                ), total=len(search_df)
            ):
                search_df_loop.append(res)

        search_df_res = pd.concat(search_df_loop)
        further_search = search_df_res[search_df_res.url != search_df_res.parent_url].drop('parent_url', axis=1).copy()
        search_df_res = search_df_res.drop('parent_url', axis=1)
        if not further_search.empty:
            search_sub = self.areas(further_search, level=level + 1)
            search_df_res = search_sub.merge(
                search_df_res,
                left_on='parent',
                right_on='area',
                how='outer',
                suffixes=[f'_bot{level}', '']
            )
        return search_df_res

    def area_wrap(self, search_df):
        res = self.areas(search_df)
        search_df_res = res.merge(
            search_df,
            left_on='parent',
            right_on='area',
            suffixes=('_bot', '')
        )
        temp = search_df_res.loc[:, search_df_res.columns.str.contains('url')].T
        search_df_res.loc[:, search_df_res.columns.str.contains('url')] = temp.fillna(method='bfill').T
        temp = search_df_res.loc[:, search_df_res.columns.str.contains('area')].T
        search_df_res.loc[:, search_df_res.columns.str.contains('area')] = temp.fillna(method='bfill').T
        temp = search_df_res.loc[:, search_df_res.columns.str.contains('parent')].T
        search_df_res.loc[:, search_df_res.columns.str.contains('parent')] = temp.fillna(method='bfill').T
        if 'url_bot1' in search_df_res.columns:
            search_df_res = search_df_res[(search_df_res.url_bot1 == search_df_res.url_bot0)].reset_index(
                drop=True
            ).copy()
        else:
            search_df_res = search_df_res[(search_df_res.url_bot == search_df_res.url_bot0)].reset_index(
                drop=True
            ).copy()
        return search_df_res

    def fetch_candidate_votes(self, area_parties):
        self.logger.info(f'Fetching personal votes')
        votes_loop = []
        with multiprocessing.Pool(self.processes) as pool:
            for res in tqdm.tqdm(
                pool.imap(
                    partial(self.fetch_personal_votes),
                    [
                        (
                            area,
                            self.base_url + area.url + area.url_letter + '.htm',
                            self.logger
                        ) for _, area in area_parties.iterrows()
                    ]
                ), total=len(area_parties)
            ):
                votes_loop.append(res)
        self.logger.info(f'Fetching personal votes done')
        votes = pd.concat(votes_loop)
        votes_merged = area_parties.merge(votes, on=['url', 'url_letter'], how='outer')
        return votes_merged

    def get_letters(self, url, region_url):
        resp = req_trys(url, self.logger)
        soup = BeautifulSoup(resp.content, "html.parser")
        results = soup.find("div", class_='col-xs-12 col-sm-6 col-md-8 content-block kmd-parti-list')
        parti_bogstaver = pd.DataFrame(
            {
                'parti': [link.text.strip() for link in results.find_all("a")],
                'url_letter': [
                    link['href'].lower().replace(
                        region_url.lower(), ''
                    ).replace('.htm', '').strip() for link in results.find_all("a")
                ]
            }
        )
        return parti_bogstaver

    def get_letters_kvrv(self, url, regions):
        parti_bogstaver_all = pd.DataFrame()
        for _, row in regions.iterrows():
            self.logger.info(f'Fetching party letters for {row.region}')
            parti_bogstaver = self.get_letters(f'{url}{row.url}.htm', row.url)
            parti_bogstaver['url'] = row.url
            parti_bogstaver_all = pd.concat([parti_bogstaver_all, parti_bogstaver])
        return parti_bogstaver_all
