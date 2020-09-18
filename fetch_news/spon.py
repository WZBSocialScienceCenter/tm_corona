import os
import re
import pickle
import json
import logging
import signal
from datetime import datetime, timedelta, time
from collections import defaultdict

import requests
from bs4 import BeautifulSoup


ARCHIVE_URL_FORMAT = 'https://www.spiegel.de/nachrichtenarchiv/artikel-{:02d}.{:02d}.{}.html'
START_DATE = datetime(2019, 10, 1)
#END_DATE = datetime(2019, 10, 3)
END_DATE = datetime(2020, 8, 31)
REQUEST_TIMEOUT_SEC = 15
ARCHIVE_CACHE = 'cache/spon_archive.pickle'
ARTICLES_CACHE = 'cache/spon_articles.pickle'
OUTPUT_JSON = 'data/spon.json'


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('spon')

abort_script = False
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


def elem_text(elem):
    t = elem.text
    for substr, repl in [('Icon: Spiegel Plus', ''), ('\xa0', ' ')]:
        t = t.replace(substr, repl)
    return t.strip()


def error(msg, obj, key=None):
    logger.error(msg)

    msg = msg.lstrip(' >')
    if key is None:
        obj['error_message'] = msg
    else:
        obj[key].append({'error_message': msg})


def handle_abort(signum, frame):
    global abort_script
    print('received signal %d – aborting script...' % signum)
    abort_script = True


for signame in ('SIGINT', 'SIGHUP', 'SIGTERM'):
    sig = getattr(signal, signame, None)
    if sig is not None:
        signal.signal(sig, handle_abort)


#%%

archive_rows = load_data_from_pickle(ARCHIVE_CACHE, defaultdict(list))

logger.info('fetching headlines and article URLs from archive')

for day in range(duration.days):
    if abort_script:
        break

    fetch_date = START_DATE + timedelta(days=day)
    fetch_date_str = fetch_date.date().isoformat()
    logger.info('> [%d/%d]: %s' % (day+1, duration.days, fetch_date_str))

    if fetch_date_str in archive_rows.keys() \
            and len(archive_rows[fetch_date_str]) > 0 \
            and 'error_message' not in archive_rows[fetch_date_str][0].keys():
        logger.info('>> already fetched this date – skipping')
        continue

    archive_url = ARCHIVE_URL_FORMAT.format(fetch_date.day, fetch_date.month, fetch_date.year)
    logger.info('>> querying %s' % archive_url)

    try:
        resp = requests.get(archive_url, timeout=REQUEST_TIMEOUT_SEC)
    except IOError:
        error('>> IO error on request', archive_rows, fetch_date_str)
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
                        error('>> no headline given', archive_rows, fetch_date_str)
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

                        articlecateg = elem_text(hfoot[2])
                    else:
                        error('>> no URL in headline link', archive_rows, fetch_date_str)
                        continue

                    archive_rows[fetch_date_str].append({
                        'archive_headline': headline,
                        'url': url,
                        'archive_retrieved': datetime.today().isoformat(timespec='seconds'),
                        'categ': articlecateg,
                        'pub_date': fetch_date_str,
                        'pub_time': pub_time.isoformat()
                    })
        else:
            error('>> unexpected number of elements in main container: %d' % len(container),
                  archive_rows, fetch_date_str)
    else:
        error('>> response not OK', archive_rows, fetch_date_str)

    logger.info('>> got %d headlines with URLs for this day' % len(archive_rows[fetch_date_str]))

    store_pickle(archive_rows, ARCHIVE_CACHE, 'archive headlines and article URLs')

if not abort_script:
    store_pickle(archive_rows, ARCHIVE_CACHE, 'archive headlines and article URLs')

#%%

articles_data = load_data_from_pickle(ARTICLES_CACHE, defaultdict(dict))

logger.info('fetching article texts')

for day, (fetch_date, day_articles) in enumerate(archive_rows.items()):
    if abort_script:
        break

    logger.info('> [%d/%d]: %s' % (day+1, len(archive_rows), fetch_date))

    for i_art, art in enumerate(day_articles):
        if abort_script:
            break

        logger.info('>> [%d/%d]: %s' % (i_art + 1, len(day_articles), art['url']))

        if not art['url'].startswith('https://www.spiegel.de'):
            logging.info('>> skipping URL that does not refer to SPON')
            continue
        if 'error_message' in art.keys():
            logger.info('>> skipping because of error when scraping archive: %s' % art['error_message'])
            continue
        if art['url'] in articles_data[fetch_date] and 'error_message' not in articles_data[fetch_date][art['url']]:
            logger.info('>> skipping because this article was already scraped')
            continue

        try:
            resp = requests.get(art['url'], timeout=REQUEST_TIMEOUT_SEC)
        except IOError:
            error('>> IO error on request', art)
            articles_data[fetch_date][art['url']] = art
            continue

        if resp.ok:
            soup = BeautifulSoup(resp.content, 'html.parser')

            if len(soup.find_all('div', attrs={'data-galleryteaser-el': 'galleryActivator'})) > 0:
                article = soup
            else:
                article = soup.select_one('main article')

            topline_headline = article.select('header h2 span')
            if len(topline_headline) < 2:
                logger.warning('>> no valid top line / headline elements')
                topline = None
                headline = None
            else:
                topline = elem_text(topline_headline[0])
                headline = elem_text(topline_headline[1])

            intro_elem = article.select_one('header div.leading-loose')
            author = None
            if intro_elem:
                intro = elem_text(intro_elem)
                author_elem = intro_elem.find_next('div')
                if author_elem:
                    author_elem = author_elem.select_one('a')
                    if author_elem:
                        author = elem_text(author_elem)
            else:
                intro = None
                logger.warning('>> no valid intro element found')

                author_elem = article.select_one('header h2').find_next('div')
                if author_elem:
                    author_elem = author_elem.select_one('a')
                    if author_elem:
                        author = elem_text(author_elem)

            # if not author:   # this is quite common
            #     logger.warning('>> no author element found')

            body_elem = article.find_all('div', attrs={'data-article-el': 'body'})
            if len(body_elem) != 1:
                error('>> no valid article body element found', art)
                articles_data[fetch_date][art['url']] = art
                continue

            body_elem = body_elem[0]
            body_pars = [elem_text(p_elem) for p_elem in body_elem.select('div.RichText p')]
            if not body_pars:   # some articles use "section" instead of "div"
                body_pars = [elem_text(p_elem) for p_elem in body_elem.select('section.RichText p')]

            logger.info('>> fetched %d paragraphs' % len(body_pars))

            if not intro and len(body_pars) > 0:
                intro = body_pars.pop(0)

            art.update({
                'retrieved': datetime.today().isoformat(timespec='seconds'),
                'topline': topline,
                'headline': headline,
                'author': author,
                'intro': intro,
                'paragraphs': body_pars
            })

            articles_data[fetch_date][art['url']] = art
        else:
            error('>> response not OK', art)
            articles_data[fetch_date][art['url']] = art
            continue

        store_pickle(articles_data, ARTICLES_CACHE, 'scraped articles')

#%%

if not abort_script:
    store_pickle(articles_data, ARTICLES_CACHE, 'scraped articles')

    logger.info('will store result to %s' % OUTPUT_JSON)

    articles_list = []
    for art_day in articles_data.values():
        articles_list.extend(art_day.values())
    del articles_data

    with open(OUTPUT_JSON, 'w') as f:
        json.dump(articles_list, f)

    logger.info('done.')
