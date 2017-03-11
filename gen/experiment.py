# -*- coding: utf-8 -*-
'''
Experiments
'''

# =============================================================================
# Experiment parameters
# =============================================================================
# Default value
DEF = 'def'
# Variation
VAR = 'var'

# Attributes
ATT = 'att'
# Sequences
NSQ = 'nsq'
# Percent of sequence identifier per instant
PSI = 'psi'

# Identifier attributes
IDA = 'ida'
# Range
RAN = 'ran'
# Slide
SLI = 'sli'

# Data parameters
DATA_PAR_LIST = [ATT, NSQ, PSI]
# Query parameters
QUERY_PAR_LIST = [IDA, RAN, SLI]
# Full parameter list
PAR_LIST = [ATT, NSQ, PSI, IDA, RAN, SLI]

# =============================================================================
# Stream attributes, types and values
# =============================================================================
# Timestamp attribute for StremPref streams and tables
TS_ATT = '_TS'
# Flag attribute for StreamPref tables
FL_ATT = '_FL'
# Integer type
INTEGER = 'INTEGER'
# Maximum attribute value
MAX_VALUE = 16

# =============================================================================
# Algorithms
# =============================================================================
# Algorithm configuration key
ALG = 'alg'
# Algorithms / equivalences
SEQ_ALG = 'seq'
CQL_ALG = 'cql'
# List of algorithms
SEQ_ALGORITHM_LIST = [SEQ_ALG, CQL_ALG]

# =============================================================================
# Experiment measures
# =============================================================================
RUNTIME = 'runtime'
MEMORY = 'memory'


def add_experiment(experiment_list, experiment):
    '''
    Add an experiment into experiment list
    '''
    if experiment not in experiment_list:
        experiment_list.append(experiment.copy())


def gen_experiment_list(parameter_conf, algorithm_list):
    '''
    Generate the list of experiments
    '''
    exp_list = []
    # Default parameters configuration
    def_conf = {key: parameter_conf[key][DEF] for key in parameter_conf}
    # For every algorithm
    for alg in algorithm_list:
        # For every parameter
        for par in parameter_conf:
            # Check if parameter has variation
            if VAR in parameter_conf[par]:
                # For every value in the variation
                for value in parameter_conf[par][VAR]:
                    # Copy default values
                    conf = def_conf.copy()
                    # Change parameter to current value
                    conf[par] = value
                    # Set the algorithm
                    conf[ALG] = alg
                    # Add to experiment list
                    add_experiment(exp_list, conf)
    return exp_list


def get_attribute_list(attributes_number, prefix='', include_timestamp=False):
    '''
    Return a list of attributes
    '''
    # Create attribute list
    att_list = [prefix + 'A' + str(num)
                for num in range(1, attributes_number + 1)]
    # Include timestamp attribute at beginning
    if include_timestamp:
        att_list.insert(0, TS_ATT)
    return att_list


def get_max_value(parameter_conf, parameter):
    '''
    Return the maximum value of a parameter variation
    '''
    return max(parameter_conf[parameter][VAR])


def get_id(experiment_conf, parameter_conf, parameter_list=None):
    '''
    Return full experiment identifier
    '''
    id_str = ''
    if parameter_list is None:
        parameter_list = PAR_LIST
    # For every parameter
    for par in parameter_list:
        # Check if current parameter has variation
        if VAR in parameter_conf[par]:
            # Get value for this parameter in the current experiment
            id_str += par + str(experiment_conf[par])
    return id_str


def get_data_id(experiment_conf, parameter_conf):
    '''
    Return experiment identifier for data
    '''
    return get_id(experiment_conf, parameter_conf, DATA_PAR_LIST)


def get_query_id(experiment_conf, parameter_conf):
    '''
    Return experiment identifier for query
    '''
    return get_id(experiment_conf, parameter_conf, QUERY_PAR_LIST)


def get_variated_parameters(parameter_conf):
    '''
    Return a list of parameters having variation
    '''
    par_list = []
    for par in parameter_conf:
        if VAR in parameter_conf[par]:
            par_list.append(par)
    return par_list
