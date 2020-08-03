import os
import re
import pickle
import logging
from datetime import datetime, timedelta, time
from collections import defaultdict

import requests
from bs4 import BeautifulSoup


ARCHIVE_URL_FORMAT = 'https://www.spiegel.de/nachrichtenarchiv/artikel-{:02d}.{:02d}.{}.html'
START_DATE = datetime(2019, 12, 15)
#END_DATE = datetime.today()
END_DATE = datetime(2019, 12, 16)
REQUEST_TIMEOUT_SEC = 15
ARCHIVE_CACHE = 'cache/spon_archive.pickle'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('spon')
duration = END_DATE - START_DATE
pttrn_time = re.compile(r'(\d+).(\d{2})\s+Uhr$')

#%%

def load_data_from_pickle(fname, nonexistent_init_data=None):
    if nonexistent_init_data is None or os.path.exists(fname):
        logger.info('loading existing data from %s' % fname)
        with open(fname, 'rb') as f:
            return pickle.load(f)
    else:
        if nonexistent_init_data is not None:
            logger.info('initializing with empty dataset')
        return nonexistent_init_data


def store_pickle(data, fname, message_label='', rotate_files=True):
    if message_label:
        logger.info('storing %s to %s' % (message_label, fname))

    if rotate_files and os.path.exists(fname):
        os.rename(fname, fname + '~')

    with open(fname, 'wb') as f:
        pickle.dump(data, f)


#%%

archive_rows = load_data_from_pickle(ARCHIVE_CACHE, defaultdict(list))

logger.info('fetching headlines and article URLs from archive')

for day in range(duration.days):
    fetch_date = START_DATE + timedelta(days=day)
    logger.info('> [%d/%d]: %s' % (day+1, duration.days, str(fetch_date.date())))

    if fetch_date in archive_rows.keys():
        logger.info('>> already fetched this date – skipping')
        continue

    archive_url = ARCHIVE_URL_FORMAT.format(fetch_date.day, fetch_date.month, fetch_date.year)

    logger.info('>> querying %s' % archive_url)

    try:
        resp = requests.get(archive_url, timeout=REQUEST_TIMEOUT_SEC)
    except IOError:
        logger.warning('>> IO error on request')
        continue

    if resp.ok:
        soup = BeautifulSoup(resp.content, 'html.parser')
        container = soup.find_all('section', attrs={'data-area': 'article-teaser-list'})

        if len(container) == 1:
            headlines_container = container[0].select('article')
            for hcont in headlines_container:
                if any(len(hcont.find_all('span', attrs={'data-conditional-flag': k})) != 0
                       for k in ('gallery', 'video', 'audio', 'paid')):
                    continue

                url = hcont.select_one('h2 a').attrs.get('href', '')
                if url:
                    headline = hcont.select_one('h2 a').attrs.get('title', '')

                    if not headline:
                        logger.warning('>> no headline given')
                        continue

                    headline = headline.replace('\xa0', ' ')
                    hfoot = hcont.select('footer span')
                    pub_time = None
                    articlecateg = None

                    if len(hfoot) == 3:
                        date_str = hfoot[0].text.strip()
                        m_time = pttrn_time.search(date_str)
                        if m_time:
                            try:
                                time_h = int(m_time.group(1))
                                time_m = int(m_time.group(2))
                                pub_time = time(time_h, time_m)
                            except ValueError:
                                logger.warning('>> invalid publication time given')
                        else:
                            logger.warning('>> no publication time given')

                        articlecateg = hfoot[2].text.strip()
                    else:
                        logger.warning('>> no URL in headline link')

                    archive_rows[fetch_date].append([headline, url, articlecateg, fetch_date.date(), pub_time])
        else:
            logger.warning('>> unexpected number of elements in main container: %d' % len(container))
    else:
        logger.warning('>> response not OK')

    logger.info('>> got %d headlines with URLs for this day' % len(archive_rows[fetch_date]))

    store_pickle(archive_rows, ARCHIVE_CACHE, 'archive headlines and article URLs')

#%%

logger.info('fetching article texts')