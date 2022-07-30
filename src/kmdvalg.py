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
        base_url_rv='https://www.kmdvalg.dk/rv/2021/',
        base_url_kv='https://www.kmdvalg.dk/kv/2021/',
        base_url_ft='https://www.kmdvalg.dk/fv/2019/',
        filename='personlige_stemmer_pr_stemmested'
    ):
        self.logger = logger
        self.base_url_rv = base_url_rv
        self.base_url_kv = base_url_kv
        self.base_url_ft = base_url_ft
        self.processes = processes
        self.filename = filename

    def scrape_general(self, valg):
        time_start = pd.Timestamp.utcnow()
        if isinstance(valg, list):
            res_all = pd.DataFrame()
            for val in valg:
                res_all = pd.concat([res_all, self.scrape_general(val)], axis=0, ignore_index=True)
            return res_all
        if valg.lower() == 'kv':
            baseurl = self.base_url_kv
            getareas = self.get_areas_kv
            og_cols = ['kommune', 'stemmested', 'parti', 'url_letter', 'name', 'personal_votes']
            new_cols = ['kommune', 'stemmested', 'parti', 'partibogstav', 'kandidat_navn', 'personlige_stemmer']
            file_ext = 'KV'
        elif valg.lower() == 'rv':
            baseurl = self.base_url_rv
            getareas = self.get_areas_rv
            og_cols = ['kommune', 'stemmested', 'region', 'parti', 'url_letter', 'name', 'personal_votes']
            new_cols = [
                'kommune', 'stemmested', 'region', 'parti', 'partibogstav', 'kandidat_navn', 'personlige_stemmer'
            ]
            file_ext = 'RV'
        elif valg.lower() == 'ft':
            baseurl = self.base_url_ft
            getareas = self.get_areas_ft
            og_cols = ['stemmested', 'kommune', 'kreds', 'storkreds', 'parti', 'url_letter', 'name', 'personal_votes']
            new_cols = [
                'stemmested', 'kommune', 'kreds', 'storkreds', 'parti', 'partibogstav', 'kandidat_navn',
                'personlige_stemmer'
            ]
            file_ext = 'FT'
        else:
            self.logger.warning(f'Invalid valg entered. Should be kv/rv/ft')
            return pd.DataFrame()
        self.logger.info(
            f'Fetching results for {file_ext}'
        )
        kmd_scraper = KMDScraper(self.logger, self.processes, baseurl)
        local_areas, hierachy, areas_with_distinct_parties = getareas(kmd_scraper)
        letters = kmd_scraper.get_letters_kvrv(f'{baseurl}', areas_with_distinct_parties)

        if valg.lower() == 'kv' or valg.lower() == 'rv':
            local_areas['masterurl'] = local_areas.url.str[0:letters.url.str.len().unique().item()]
            area_parties = local_areas.merge(
                letters, left_on='masterurl', right_on='url', suffixes=('', '_w'), how='left'
            ).drop(['masterurl', 'url_w'], axis=1)
        elif valg.lower() == 'ft':
            local_areas['tmp'] = letters['tmp'] = 1
            area_parties = local_areas.merge(letters.drop('url', axis=1), on='tmp').drop('tmp', axis=1)
        else:
            return pd.DataFrame()

        votes = kmd_scraper.fetch_candidate_votes(area_parties)
        votes_hierachy = votes.merge(hierachy, on='url')
        votes_csv_ready = votes_hierachy[og_cols].copy()
        votes_csv_ready.columns = new_cols
        votes_csv_ready.to_csv(f'{self.filename}_{file_ext}.csv')
        self.logger.info(
            f'Fetched {file_ext} results in {np.round((pd.Timestamp.utcnow() - time_start).value / 1000000000, 2)} '
            f'seconds'
        )
        return votes_csv_ready

    def get_areas_ft(self, kmd_scraper, url='https://www.kmdvalg.dk/fv/2019/KMDValgFV.html'):
        storkredse_df = self.get_df(url)
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
        ft_letter_areas = pd.DataFrame(
            {
                'area': ['Denmark'],
                'url': ['F1003'],
                'region': ['Denmark']
            }
        )
        return bot_res, hierachy, ft_letter_areas

    def get_areas_rv(self, kmd_scraper, url='https://www.kmdvalg.dk/rv/2021/KMDValgRV.html'):
        regioner_df = self.get_df(url)
        regioner_df['region'] = np.where(
            regioner_df.url.str.len() == regioner_df.url.str.len().min(),
            regioner_df.area,
            np.nan
        )
        regioner_df['region'] = regioner_df['region'].fillna(method='ffill')
        region_url = regioner_df[(regioner_df.url.str.len() == regioner_df.url.str.len().min())].copy()
        kommuner_df = regioner_df[~(regioner_df.url.str.len() == regioner_df.url.str.len().min())].copy()
        search_df_res = kmd_scraper.area_wrap(kommuner_df)

        hierachy = search_df_res.loc[:, search_df_res.columns.str.contains(
            'parent'
        ) | search_df_res.columns.str.contains('region')
        ].copy().sort_values(['region', 'parent'])
        hierachy.columns = ['stemmested', 'kommune', 'region']
        hierachy['url'] = search_df_res.iloc[:, 1:2]
        bot_res = search_df_res.iloc[:, :2].copy()
        bot_res.columns = ['area', 'url']
        return bot_res, hierachy, region_url

    def get_areas_kv(self, kmd_scraper, url='https://www.kmdvalg.dk/kv/2021/KMDValgKV.html'):
        kommuner_df = self.get_df(url)

        kommuner_df = kommuner_df[~kommuner_df.url.isna()].copy().reset_index(drop=True)
        search_df_res = kmd_scraper.area_wrap(kommuner_df)

        hierachy = search_df_res.loc[:, search_df_res.columns.str.contains(
            'parent'
        )].copy()
        hierachy.columns = ['stemmested', 'kommune']
        kommuner_df.columns = ['region', 'url']
        hierachy['url'] = search_df_res.iloc[:, 1:2]
        bot_res = search_df_res.iloc[:, :2].copy()
        bot_res.columns = ['area', 'url']
        return bot_res, hierachy, kommuner_df

    def get_df(self, url):
        resp = req_trys(url, self.logger)
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
        return storkredse_df
