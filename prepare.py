import re
import json
import logging

#from tmtoolkit.corpus import Corpus
from tmtoolkit.preprocess import TMPreproc
from tmtoolkit.utils import pickle_data

logging.basicConfig(level=logging.INFO)
tmtoolkit_log = logging.getLogger('tmtoolkit')
tmtoolkit_log.setLevel(logging.INFO)
tmtoolkit_log.propagate = True

INPUT_DATA = 'fetch_news/data/spon.json'
OUTPUT_DTM = 'data/dtm.pickle'

pttrn_urlend = re.compile(r'\.html?$')

#%%

print(f'loading articles from {INPUT_DATA}')

with open(INPUT_DATA) as f:
    sponraw = json.load(f)

print(f'loaded {len(sponraw)} articles')

#%%

corpus = {}
meta = {}

print('generating corpus')

for art in sponraw:
    if 'error_message' in art:
        print('error for article', art['url'], ':', art['error_message'])
        continue

    if pttrn_urlend.search(art['url']):
        urlend = art['url'].rindex('.')
    else:
        urlend = None

    doclabel = art['url'][art['url'].rindex('/')+1:urlend]
    doctext = '\n\n'.join([art['archive_headline'], art['intro'] or '', '\n\n'.join(art['paragraphs'])])

    if doclabel in corpus.keys():
        print(f'> ignoring duplicate: {doclabel}')
    else:
        corpus[doclabel] = doctext
        assert doclabel not in meta.keys()
        meta[doclabel] = {k: v for k, v in art.items() if k in {'categ', 'pub_date', 'author'}}

print(f'generated corpus with {len(corpus)} documents')

#corpus = Corpus(corpus).sample(10000)
#print(f'sampled {len(corpus)} documents')

del sponraw

#%%

print('tokenizing documents')

preproc = TMPreproc(corpus, language='de')
del corpus

preproc.print_summary()

print('processing documents')

preproc.pos_tag() \
    .lemmatize() \
    .tokens_to_lowercase() \
    .remove_special_chars_in_tokens() \
    .clean_tokens(remove_shorter_than=2, remove_numbers=True) \
    .remove_common_tokens(df_threshold=0.9) \
    .remove_uncommon_tokens(df_threshold=0.05)

#%%

preproc.print_summary()

#%%

print('generating DTM')

dtm = preproc.dtm
print(f'DTM shape: {dtm.shape}')

#%%

print('storing output DTM to {OUTPUT_DTM}')

pickle_data((preproc.doc_labels, preproc.vocabulary, dtm), OUTPUT_DTM)

print('done.')

