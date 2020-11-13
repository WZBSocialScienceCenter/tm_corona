from pprint import pprint
from collections import defaultdict

from tmtoolkit.topicmod.tm_lda import evaluate_topic_models
from tmtoolkit.topicmod.evaluate import results_by_parameter
from tmtoolkit.topicmod.visualize import plot_eval_results
from tmtoolkit.utils import unpickle_file, pickle_data


INPUT_DTM = 'data/dtm.pickle'
OUTPUT_EVAL_PLOT = 'plots/tm_final_eta%s.png'
OUTPUT_EVAL_RESULTS = 'data/tm_final_results.pickle'

#%%


print(f'loading DTM from {INPUT_DTM} ...')
doc_labels, vocab, dtm = unpickle_file(INPUT_DTM)
assert len(doc_labels) == dtm.shape[0]
assert len(vocab) == dtm.shape[1]
print(f'loaded DTM with shape {dtm.shape} and {dtm.sum()} tokens')

#%%

const_params = {
    'n_iter': 2000,
    'random_state': 20200918  # to make results reproducible
}

print('constant parameters:')
print(const_params)


eta = [0.7, 0.9]
n_topics = [180, 200, 220]


var_params = [{'eta': e, 'n_topics': k, 'alpha': 10.0/k}
              for e in eta
              for k in n_topics]
   
print('varying parameters:')
pprint(var_params)

metrics = [
    'cao_juan_2009',
    'arun_2010',
    'coherence_mimno_2011',
]
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

eval_results_per_eta = defaultdict(list)

for params, res in eval_results:
    eval_results_per_eta[params['eta']].append((params, res))

#%%

for e, eval_results_eta in eval_results_per_eta.items():
    eval_results_by_topics = results_by_parameter(eval_results_eta, 'n_topics')
    fig, axes = plot_eval_results(eval_results_by_topics)

    eval_plot_file = OUTPUT_EVAL_PLOT % str(e)
    print(f'storing evaluation results plot to {eval_plot_file}')
    fig.savefig(eval_plot_file)


#%%

print('done.')