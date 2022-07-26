import time

import numpy as np
import requests


def req_trys(url, logger, method='get', max_trys=10):
    trys = 0
    max_trys -= 1
    while trys <= max_trys:
        try:
            with requests.Session() as s:
                if method == 'get':
                    resp = s.get(url)
                elif method == 'post':
                    resp = s.post(url)
            resp.raise_for_status()
            return resp
        except:
            trys += 1
            if trys <= max_trys:
                timer = np.random.uniform(0, 4, 1)[0]
                if trys > 6:
                    logger.warning(
                        f'Failed fetching from {url} {trys} time(s), trying again in {np.round(timer, 2)} seconds'
                    )
                time.sleep(timer)
            continue
    logger.error(f'Failed fetching from {url} {max_trys + 1} times, skipping')
