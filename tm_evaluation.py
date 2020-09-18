from pprint import pprint

from tmtoolkit.topicmod.tm_lda import AVAILABLE_METRICS, evaluate_topic_models
from tmtoolkit.topicmod.evaluate import results_by_parameter
from tmtoolkit.topicmod.visualize import plot_eval_results
from tmtoolkit.utils import unpickle_file, pickle_data


INPUT_DTM = 'data/dtm.pickle'
OUTPUT_EVAL_PLOT = 'plots/tm_eval.png'
OUTPUT_EVAL_RESULTS = 'data/tm_eval_results.pickle'

#%%

print(f'loading DTM from {INPUT_DTM} ...')
doc_labels, vocab, dtm = unpickle_file(INPUT_DTM)
assert len(doc_labels) == dtm.shape[0]
assert len(vocab) == dtm.shape[1]
print(f'loaded DTM with shape {dtm.shape} and {dtm.sum()} tokens')

#%%

const_params = {
    'n_iter': 1500,
    'eta': 0.1,       # "eta" aka "beta"
    'random_state': 20200918  # to make results reproducible
}

print('constant parameters:')
print(const_params)

var_params = [{'n_topics': k, 'alpha': 1/k} for k in range(20, 151, 10)]
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
                                     return_models=True,
                                     metric=metrics)

#%%

print(f'storing evaluation results to {OUTPUT_EVAL_RESULTS}')
pickle_data(eval_results, OUTPUT_EVAL_RESULTS)

#%%

eval_results_by_topics = results_by_parameter(eval_results, 'n_topics')

fig, axes = plot_eval_results(eval_results_by_topics)
#fig.show()
print(f'storing evaluation results plot to {OUTPUT_EVAL_PLOT}')
fig.savefig(OUTPUT_EVAL_PLOT)

print('done.')
