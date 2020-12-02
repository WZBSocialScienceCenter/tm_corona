"""
Text data preparation for SPON news corpus.

This uses tmtoolkit (see https://tmtoolkit.readthedocs.io/) for preprocessing the large text corpus and generating
a document-term matrix, which is then used as input for topic modeling (see `tm_evaluation.py` and `tm_final.py`).

Note that this requires quite a large amount of computer memory to run (> 8GB).

December 2020, Markus Konrad <markus.konrad@wzb.eu>
"""

import re
import json
import logging

from tmtoolkit.preprocess import TMPreproc
from tmtoolkit.utils import pickle_data

# enable logging for tmtoolkit
logging.basicConfig(level=logging.INFO)
tmtoolkit_log = logging.getLogger('tmtoolkit')
tmtoolkit_log.setLevel(logging.INFO)
tmtoolkit_log.propagate = True


#%% configuration and constants

INPUT_DATA = 'fetch_news/data/spon.json'    # fetched SPON corpus raw data
OUTPUT_DTM = 'data/dtm_nov20.pickle'              # document-term matrix output
OUTPUT_META = 'data/meta_nov20.pickle'            # corpus metadata
OUTPUT_CORPUS = 'data/corpus_nov20.pickle'        #

pttrn_urlend = re.compile(r'\.html?$')


#%% loading raw data from JSON file

print(f'loading articles from {INPUT_DATA}')

with open(INPUT_DATA) as f:
    sponraw = json.load(f)

print(f'loaded {len(sponraw)} articles')

#%% generating the corpus of raw text and corpus metadata

corpus = {}     # maps document label to raw article text (including headline, abstract and main text)
meta = {}       # maps document label to document metadata such as category, publication date, author

print('generating corpus')

# iterate through scraped article data
for art in sponraw:
    if 'error_message' in art:      # skip articles with errors
        print('error for article', art['url'], ':', art['error_message'])
        continue

    # generate document label from end of article URL
    if pttrn_urlend.search(art['url']):
        urlend = art['url'].rindex('.')
    else:
        urlend = None

    doclabel = art['url'][art['url'].rindex('/')+1:urlend]

    # generate document full text from headline, abstract and paragraphs, all separted by double linebreaks
    doctext = '\n\n'.join([art['archive_headline'], art['intro'] or '', '\n\n'.join(art['paragraphs'])])

    # store to corpus and metadata dicts
    if doclabel in corpus.keys():
        print(f'> ignoring duplicate: {doclabel}')
    else:
        corpus[doclabel] = doctext
        assert doclabel not in meta.keys()
        meta[doclabel] = {k: v for k, v in art.items() if k in {'categ', 'pub_date', 'author'}}

print(f'generated corpus with {len(corpus)} documents')

del sponraw     # remove unused objects

# store corpus and metadata to disk
print(f'storing corpus to {OUTPUT_CORPUS}')
pickle_data(corpus, OUTPUT_CORPUS)
print(f'storing corpus metadata to {OUTPUT_META}')
pickle_data(meta, OUTPUT_META)

del meta        # remove unused objects


#%% process text data and form document-term matrix using parallel text processing via TMPreproc

print('tokenizing documents')

preproc = TMPreproc(corpus, language='de')
del corpus      # remove unused objects

preproc.print_summary()

print('processing documents')

# run preprocessing pipeline
# last two steps remove tokens that appear in more than 95% or less than 1% of all documents
preproc.pos_tag() \
    .lemmatize() \
    .tokens_to_lowercase() \
    .remove_special_chars_in_tokens() \
    .clean_tokens(remove_shorter_than=2, remove_numbers=True) \
    .remove_common_tokens(df_threshold=0.95) \
    .remove_uncommon_tokens(df_threshold=0.01)

preproc.print_summary()

print('generating DTM')

dtm = preproc.dtm
print(f'DTM shape: {dtm.shape}')


#%% store document-term matrix along with document labels and vocabulary to disk

print(f'storing output DTM to {OUTPUT_DTM}')
pickle_data((preproc.doc_labels, preproc.vocabulary, dtm), OUTPUT_DTM)

print('done.')
