"""
Topic model evaluation for parameter tuning.

This uses tmtoolkit (see https://tmtoolkit.readthedocs.io/) for parallel topic model evaluation. The script accepts a
parameter `eta` that is held constant while a set of alpha parameter values and numbers of topics varies during model
evalation.

November 2020, Markus Konrad <markus.konrad@wzb.eu>
"""

import sys

from pprint import pprint

from tmtoolkit.topicmod.tm_lda import AVAILABLE_METRICS, evaluate_topic_models
from tmtoolkit.topicmod.evaluate import results_by_parameter
from tmtoolkit.topicmod.visualize import plot_eval_results
from tmtoolkit.utils import unpickle_file, pickle_data

#%% configuration and constants

INPUT_DTM = 'data/dtm.pickle'
OUTPUT_EVAL_PLOT = 'plots/tm_eval_eta%s.png'
OUTPUT_EVAL_RESULTS = 'data/tm_eval_results_eta%s.pickle'


#%% handle eta parameter passed as script argument

if len(sys.argv) < 2:
    print('must pass argument: eta')
    exit(1)

eta_str = sys.argv[1]
eta = float(eta_str)
print(f'will run evaluation with eta={eta}')


#%% load input document-term matrix

print(f'loading DTM from {INPUT_DTM} ...')
doc_labels, vocab, dtm = unpickle_file(INPUT_DTM)
assert len(doc_labels) == dtm.shape[0]
assert len(vocab) == dtm.shape[1]
print(f'loaded DTM with shape {dtm.shape} and {dtm.sum()} tokens')

del doc_labels, vocab


#%% set up parameters and run model evaluation

const_params = {
    'n_iter': 2000,
    'eta': eta,       # "eta" aka "beta"
    'random_state': 20200918  # to make results reproducible
}

print('constant parameters:')
print(const_params)

#var_params = [{'n_topics': k, 'alpha': (eta*100)/k} for k in range(20, 201, 10) if (eta*100)/k < 1]
var_params = [{'n_topics': k, 'alpha': 10.0/k}
              for k in list(range(20, 101, 20)) + [125, 150, 175, 200, 250, 300, 400, 500, 600, 800, 1000]]
print('varying parameters:')
pprint(var_params)

metrics = list(AVAILABLE_METRICS)
metrics.pop(metrics.index('loglikelihood'))
print('used metrics:')
print(metrics)

print(f'evaluating {len(var_params)} models...')

eval_results = evaluate_topic_models(dtm,
                                     varying_parameters=var_params,
                                     constant_parameters=const_params,
                                     return_models=False,
                                     metric=metrics)

#%% store evaluation results

eval_results_file = OUTPUT_EVAL_RESULTS % eta_str
print(f'storing evaluation results to {eval_results_file}')
pickle_data(eval_results, eval_results_file)

#%% plot evaluation results and store in `plots` folder

eval_results_by_topics = results_by_parameter(eval_results, 'n_topics')

fig, axes = plot_eval_results(eval_results_by_topics)
#fig.show()

eval_plot_file = OUTPUT_EVAL_PLOT % eta_str
print(f'storing evaluation results plot to {eval_plot_file}')
fig.savefig(eval_plot_file)

print('done.')
