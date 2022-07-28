import time

import numpy as np
import requests


def req_trys(url, logger, method='get'):
    trys = 0
    while trys <= 5:
        try:
            with requests.Session() as s:
                if method == 'get':
                    resp = s.get(url)
                elif method == 'post':
                    resp = s.post(url)
            return resp
        except:
            trys += 1
            timer = np.random.uniform(0, 4, 1)[0]
            logger.warning(f'Failed fetching from {url} {trys} times, trying again in {np.round(timer, 2)} seconds')
            time.sleep(timer)
            continue
    logger.error(f'Failed fetching from {url} 5 times')
