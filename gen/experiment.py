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
# Rules
RUL = 'rul'
# Preference level
LEV = 'lev'
# Indifferent attributes
IND = 'ind'

# Data parameters
DATA_PAR_LIST = [ATT, NSQ, PSI]
# # Query parameters
# QUERY_PAR_LIST = [IDA, RAN, SLI, RUL, LEV, IND]
# # Full parameter list
# PAR_LIST = [ATT, NSQ, PSI, IDA, RAN, SLI, RUL, LEV, IND]

# =============================================================================
# Configuration keys
# =============================================================================
# Algorithm
ALGORITHM = 'algo'
# Directory
DIRECTORY = 'direc'
# Parameter
PARAMETER = 'dir'

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
# CQL equivalence
CQL_ALG = 'cql'
# SEQ operator
SEQ_ALG = 'seq'
# =============================================================================
# TPref BNL search
BNL_SEARCH = 'bnl_search'
# Incremental partition sequence tree
INC_PARTITION_SEQTREE_ALG = 'inc_partition_seqtree'
# Incremental partition sequence tree (with pruning)
INC_PARTITION_SEQTREE_PRUNING_ALG = 'inc_partition_seqtree_pruning'
# Incremental partition list sequence tree
INC_PARTITIONLIST_SEQTREE_ALG = 'inc_partitionlist_seqtree'
# Incremental partition list sequence tree (with pruning)
INC_PARTITIONLIST_SEQTREE_PRUNING_ALG = 'inc_partitionlist_seqtree_pruning'
# =============================================================================
# # Algorithm lists
# # SEQ algorithms
# SEQ_ALGORITHM_LIST = [SEQ_ALG, CQL_ALG]
# # TPref algorithms
# TPREF_ALGORITHM_LIST = \
#     [CQL_ALG, BNL_SEARCH, INC_PARTITION_SEQTREE_ALG,
#      INC_PARTITION_SEQTREE_PRUNING_ALG, INC_PARTITIONLIST_SEQTREE_ALG,
#      INC_PARTITIONLIST_SEQTREE_PRUNING_ALG]


# =============================================================================
# Algorithm options
# =============================================================================
# Option for temporal preference algorithms
TPREF_RUN_OPTION = 'tpref'

# =============================================================================
# Experiment measures
# =============================================================================
# StreamPref measures
RUNTIME = 'runtime'
MEMORY = 'memory'
# Summary measures
SUM_RUN = 'run'
SUM_MEM = 'mem'


def add_experiment(experiment_list, experiment):
    '''
    Add an experiment into experiment list
    '''
    if experiment not in experiment_list:
        experiment_list.append(experiment.copy())


def gen_experiment_list(configuration):
    '''
    Generate the list of experiments
    '''
    exp_list = []
    parameter_conf = configuration[PARAMETER]
    # Default parameters configuration
    def_conf = get_default_experiment(parameter_conf)
    # For every algorithm
    for alg in configuration[ALGORITHM]:
        # For every parameter
        for par in parameter_conf:
            # Check if parameter has variation
            if VAR in parameter_conf[par]:
                # For every value in the variation
                for value in parameter_conf[par][VAR]:
                    # Copy default values
                    conf = def_conf.copy()
                    # Set algorithm
                    conf[ALGORITHM] = alg
                    # Change parameter to current value
                    conf[par] = value
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


def get_id(experiment_conf, parameter_conf):
    '''
    Return full experiment identifier
    '''
    id_str = ''
    parameter_list = get_varied_parameters(parameter_conf)
    # For every parameter
    for par in parameter_list:
        # Check if current parameter has variation
        if par in parameter_conf and VAR in parameter_conf[par]:
            # Get value for this parameter in the current experiment
            id_str += par + str(experiment_conf[par])
    return id_str


def get_data_id(experiment_conf):
    '''
    Return experiment identifier for data
    '''
    id_str = ''
    for par in DATA_PAR_LIST:
        id_str += par + str(experiment_conf[par])
    return id_str


def get_max_data_timestamp(parameter_conf):
    '''
    Return the maximum timstamp for a generated data stream
    '''
    return max(parameter_conf[RAN][VAR]) + max(parameter_conf[SLI][VAR])

# def get_query_id(experiment_conf, parameter_conf):
#     '''
#     Return experiment identifier for query
#     '''
#     return get_id(experiment_conf, parameter_conf, QUERY_PAR_LIST)


def get_varied_parameters(parameter_conf):
    '''
    Return a list of parameters having variation
    '''
    par_list = []
    for par in parameter_conf:
        if VAR in parameter_conf[par]:
            par_list.append(par)
    return par_list


def get_default_experiment(parameter_conf):
    '''
    Get a experiment with default values for parameters
    '''
#     par_list = get_varied_parameters(parameter_conf)
    par_list = [par for par in parameter_conf]
    return {par: parameter_conf[par][DEF] for par in par_list}
