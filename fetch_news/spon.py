"""
Web scraper for SPON news archive (https://www.spiegel.de/nachrichtenarchiv/).

November 2020, Markus Konrad <markus.konrad@wzb.eu>
"""

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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('spon')

#%% constants and configuration

ARCHIVE_URL_FORMAT = 'https://www.spiegel.de/nachrichtenarchiv/artikel-{:02d}.{:02d}.{}.html'

# start day for archive retrieval
START_DATE = datetime(2019, 6, 1)
# last day for archive retrieval
END_DATE = datetime(2020, 11, 24)

# maximum request timeout until exception is raised
REQUEST_TIMEOUT_SEC = 15

# cache file for daily archive overview data
# existing data will be loaded from this file; if archive data for a given date exists in the cache,
# it will not be fetched again
ARCHIVE_CACHE = 'cache/spon_archive.pickle'

# cache file for individual news articles
# existing data will be loaded from this file; if article data exists in the cache,
# it will not be fetched again
ARTICLES_CACHE = 'cache/spon_articles.pickle'

# output data file as JSON
OUTPUT_JSON = 'data/spon.json'


# global variable that is set to True if the script is aborted by OS (e.g. by pressing Ctrl-C)
# this makes sure that the script is not interrupted *while* data is stored to disk which would end up
# in a corrupted file
abort_script = False
duration = END_DATE - START_DATE   # timedelta
pttrn_time = re.compile(r'(\d+).(\d{2})\s+Uhr$')   # RE pattern for time component


#%% helper functions

def load_data_from_pickle(fname, nonexistent_init_data=None):
    """
    Load pickled data from file `fname` and return loaded data. If it doesn't exist, return `nonexistent_init_data`.
    """
    if nonexistent_init_data is None or os.path.exists(fname):
        logger.info('loading existing data from %s' % fname)
        with open(fname, 'rb') as f:
            return pickle.load(f)
    else:
        if nonexistent_init_data is not None:
            logger.info('initializing with empty dataset')
        return nonexistent_init_data


def store_pickle(data, fname, message_label='', rotate_files=True):
    """
     Store `data` to pickle file `fname` and print `message_label`. If `rotate_files` is True and `fname` already
     exists, rename that file to `fname~` at first.
    """
    if message_label:
        logger.info('storing %s to %s' % (message_label, fname))

    if rotate_files and os.path.exists(fname):
        os.rename(fname, fname + '~')

    with open(fname, 'wb') as f:
        pickle.dump(data, f)


def elem_text(elem):
    """
    Retrieve stripped text from an BeautfilSoup HTML element.
    """
    t = elem.text
    for substr, repl in [('Icon: Spiegel Plus', ''), ('\xa0', ' ')]:
        t = t.replace(substr, repl)
    return t.strip()


def error(msg, obj, key=None):
    """
    Print error message `msg` to logger and store error message in data item `obj`.
    """
    logger.error(msg)

    msg = msg.lstrip(' >')
    if key is None:
        obj['error_message'] = msg
    else:
        obj[key].append({'error_message': msg})


def handle_abort(signum, frame):
    """Handler for OS signals to abort script. Sets global `abort_script` to True."""
    global abort_script
    print('received signal %d – aborting script...' % signum)
    abort_script = True


# setup handler for OS signals that kill this script
for signame in ('SIGINT', 'SIGHUP', 'SIGTERM'):
    sig = getattr(signal, signame, None)
    if sig is not None:
        signal.signal(sig, handle_abort)


#%% fetch article URLs, headline, date and other article metadata from daily archive overview

# load already existing data from cache or initialize with defaultdict
# `archive_rows` maps a date string to a list of article metadata for all articles published on that day
archive_rows = load_data_from_pickle(ARCHIVE_CACHE, defaultdict(list))

logger.info('fetching headlines and article URLs from archive')

# loop through the days in the specified timespan
for day in range(duration.days):
    if abort_script:    # if interrupted by OS, break loop
        break

    # construct archive date
    fetch_date = START_DATE + timedelta(days=day)
    fetch_date_str = fetch_date.date().isoformat()
    logger.info('> [%d/%d]: %s' % (day+1, duration.days, fetch_date_str))

    # check if data already exists
    if fetch_date_str in archive_rows.keys() \
            and len(archive_rows[fetch_date_str]) > 0 \
            and 'error_message' not in archive_rows[fetch_date_str][0].keys():
        logger.info('>> already fetched this date – skipping')
        continue

    # construct URL to fetch data from
    archive_url = ARCHIVE_URL_FORMAT.format(fetch_date.day, fetch_date.month, fetch_date.year)
    logger.info('>> querying %s' % archive_url)

    # fetch archive page for that date via GET request
    try:
        resp = requests.get(archive_url, timeout=REQUEST_TIMEOUT_SEC)
    except IOError:
        error('>> IO error on request', archive_rows, fetch_date_str)
        continue

    if resp.ok:   # got page data
        # parse page
        soup = BeautifulSoup(resp.content, 'html.parser')
        container = soup.find_all('section', attrs={'data-area': 'article-teaser-list'})

        if len(container) == 1:  # we expect a single container that holds all of this day's articles teasers
            headlines_container = container[0].select('article')
            for hcont in headlines_container:   # iterate through article teasers
                # skip gallery, video, audio, paid content or ads
                if any(len(hcont.find_all('span', attrs={'data-conditional-flag': k})) != 0
                       for k in ('gallery', 'video', 'audio', 'paid')) or 'ANZEIGE' in hcont.text:
                    continue

                # get the URL to the full article
                title_elem = hcont.select_one('h2 a')
                if title_elem is None:
                    url = None
                else:
                    url = title_elem.attrs.get('href', '')

                if url:
                    # get headline
                    headline = title_elem.attrs.get('title', '')

                    if not headline:
                        error('>> no headline given', archive_rows, fetch_date_str)
                        continue

                    headline = headline.replace('\xa0', ' ')
                    hfoot = hcont.select('footer span')
                    pub_time = None
                    articlecateg = None

                    # parse metadata from teaser footer
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
                        error('>> no valid teaser footer', archive_rows, fetch_date_str)
                        continue

                    # add all fetched metadata for this article at this date
                    archive_rows[fetch_date_str].append({
                        'archive_headline': headline,
                        'url': url,
                        'archive_retrieved': datetime.today().isoformat(timespec='seconds'),
                        'categ': articlecateg,
                        'pub_date': fetch_date_str,
                        'pub_time': pub_time.isoformat()
                    })
                else:
                    error('>> no URL in headline link', archive_rows, fetch_date_str)
        else:
            error('>> unexpected number of elements in main container: %d' % len(container),
                  archive_rows, fetch_date_str)
    else:
        error('>> response not OK', archive_rows, fetch_date_str)

    logger.info('>> got %d headlines with URLs for this day' % len(archive_rows[fetch_date_str]))

    # store the data that was fetched so far
    store_pickle(archive_rows, ARCHIVE_CACHE, 'archive headlines and article URLs')

# final storage of fetched archive data
if not abort_script:
    store_pickle(archive_rows, ARCHIVE_CACHE, 'archive headlines and article URLs')


#%% fetch full article text for each article from the archive

# load already existing data from cache or initialize with defaultdict
# `articles_data` maps a date string for the publication date of an article to a dict that maps article URLs
# to individual article contents, e.g. publication date -> article URL -> article data
articles_data = load_data_from_pickle(ARTICLES_CACHE, defaultdict(dict))

logger.info('fetching article texts')

# iterate through the fetched archive data
for day, (fetch_date, day_articles) in enumerate(archive_rows.items()):
    if abort_script:    # if interrupted by OS, break loop
        break

    logger.info('> [%d/%d]: %s' % (day+1, len(archive_rows), fetch_date))

    # iterate through articles published on that day as listed in the archive
    # we will populate the article data `art` with additional data from the full article page such as the article text
    for i_art, art in enumerate(day_articles):
        if abort_script:    # if interrupted by OS, break loop
            break

        logger.info('>> [%d/%d]: %s' % (i_art + 1, len(day_articles), art['url']))

        # if an article URL doesn't point to SPON (but to Bento or other partner websites), skip it
        if not art['url'].startswith('https://www.spiegel.de'):
            logging.info('>> skipping URL that does not refer to SPON')
            continue

        # if an error happened when fetching this article's metadata from the archive, skip it
        if 'error_message' in art.keys():
            logger.info('>> skipping because of error when scraping archive: %s' % art['error_message'])
            continue

        # if we already have this article's full data in the cache and it doesn't contain an error message, skip it
        if art['url'] in articles_data[fetch_date] and 'error_message' not in articles_data[fetch_date][art['url']]:
            logger.info('>> skipping because this article was already scraped')
            continue

        # fetch full article page via GET request
        try:
            resp = requests.get(art['url'], timeout=REQUEST_TIMEOUT_SEC)
        except IOError:
            error('>> IO error on request', art)
            articles_data[fetch_date][art['url']] = art
            continue

        if resp.ok:     # got page data
            # now parse the article page
            soup = BeautifulSoup(resp.content, 'html.parser')

            if len(soup.find_all('div', attrs={'data-galleryteaser-el': 'galleryActivator'})) > 0:
                # some articles also feature a gallery, we need to set the base article element differently then
                article = soup
            else:
                article = soup.select_one('main article')

            if article is None:
                error('>> no valid article element found', art)
                articles_data[fetch_date][art['url']] = art
                continue

            # parse the text line above the headline and the headline itself
            topline_headline = article.select('header h2 span')
            if len(topline_headline) < 2:
                logger.warning('>> no valid top line / headline elements')
                topline = None
                headline = None
            else:
                topline = elem_text(topline_headline[0])
                headline = elem_text(topline_headline[1])

            # parse the introduction/abstract which also contains the author data (if printed)
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

            # parse the article main text ("body")
            body_elem = article.find_all('div', attrs={'data-article-el': 'body'})
            if len(body_elem) != 1:
                error('>> no valid article body element found', art)
                articles_data[fetch_date][art['url']] = art
                continue

            # iterate through paragraphs
            body_elem = body_elem[0]
            body_pars = [elem_text(p_elem) for p_elem in body_elem.select('div.RichText p')]
            if not body_pars:   # some articles use "section" instead of "div"
                body_pars = [elem_text(p_elem) for p_elem in body_elem.select('section.RichText p')]

            logger.info('>> fetched %d paragraphs' % len(body_pars))

            # sometimes the first paragraph resembles the intro/"abstract"
            if not intro and len(body_pars) > 0:
                intro = body_pars.pop(0)

            # add all this new information
            art.update({
                'retrieved': datetime.today().isoformat(timespec='seconds'),
                'topline': topline,
                'headline': headline,
                'author': author,
                'intro': intro,
                'paragraphs': body_pars
            })

            # add it to the dict that contains all articles' full data
            articles_data[fetch_date][art['url']] = art
        else:
            error('>> response not OK', art)
            # add this article *with an error message* to the full data
            articles_data[fetch_date][art['url']] = art
            continue

        # store the data that was fetched so far
        store_pickle(articles_data, ARTICLES_CACHE, 'scraped articles')

#%%

if not abort_script:
    # final storage of fetched full article data to cache
    store_pickle(articles_data, ARTICLES_CACHE, 'scraped articles')

    # now construct data for JSON output
    logger.info('will store result to %s' % OUTPUT_JSON)

    articles_list = []
    for art_day in articles_data.values():
        articles_list.extend(art_day.values())
    del articles_data

    with open(OUTPUT_JSON, 'w') as f:
        json.dump(articles_list, f)

    logger.info('done.')
