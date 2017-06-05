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
# Percent of consecutive timestamps
PCT = 'pct'

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

# Minimum size of sequences
MIN = 'min'
# Maximum size of sequences
MAX = 'max'

# Data parameters
DATA_PAR_LIST = [ATT, NSQ, PCT]

# =============================================================================
# Configuration keys
# =============================================================================
# Algorithm list
ALGORITHM_LIST = 'algo_list'
# Directory
DIRECTORY = 'direc'
# Parameter
PARAMETER = 'dir'
# Algorithm
ALGORITHM = 'algo'
# List of operators
OPERATOR_LIST = 'operator_list'
# Maximum attribute value
MAX_VALUE = 'max_value'
# Percent of sequence identifier per instant
TUPLE_RATE = 'tup_rate'

# =============================================================================
# Stream attributes and types
# =============================================================================
# Timestamp attribute for StremPref streams and tables
TS_ATT = '_TS'
# Flag attribute for StreamPref tables
FL_ATT = '_FL'
# Integer type
INTEGER = 'INTEGER'

# =============================================================================
# Algorithms
# =============================================================================
# CQL equivalence
CQL_ALG = 'cql'
# =============================================================================
# Algorithm SEQ operator
SEQ_ALG = 'seq'
# =============================================================================
# Algorithms for temporal Preference Operators
# BNL search
BNL_SEARCH_ALG = 'bnl_search'
# Incremental partition sequence tree
INC_PARTITION_SEQTREE_ALG = 'inc_partition_seqtree'
# Incremental partition sequence tree (with pruning)
INC_PARTITION_SEQTREE_PRUNING_ALG = 'inc_partition_seqtree_pruning'
# Incremental partition list sequence tree
INC_PARTITIONLIST_SEQTREE_ALG = 'inc_partitionlist_seqtree'
# Incremental partition list sequence tree (with pruning)
INC_PARTITIONLIST_SEQTREE_PRUNING_ALG = 'inc_partitionlist_seqtree_pruning'
# =============================================================================
# Algorithms for subsequence operators
NAIVE_SUBSEQ_ALG = "naive"
INC_SUBSEQ_ALG = "incremental"
# =============================================================================
# Algorithms for MINSEQ and MAXSEQ operator
MINSEQ_ALG = "minseq"
MAXSEQ_ALG = "maxseq"

# =============================================================================
# Experiment measures
# =============================================================================
# StreamPref measures
RUNTIME = 'runtime'
MEMORY = 'memory'
# Summary measures
SUM_RUN = 'run'
SUM_MEM = 'mem'

# =============================================================================
# Operators
# =============================================================================
SEQ = 'SEQ'
CONSEQ = 'CONSEQ'
ENDSEQ = 'ENDSEQ'
MINSEQ = 'MINSEQ'
MAXSEQ = 'MAXSEQ'
BESTSEQ = 'BESTSEQ'

# =============================================================================
# Query for statistical experiments
# =============================================================================
Q_SEQ = [SEQ]
Q_SEQ_CONSEQ = [SEQ, CONSEQ]
Q_SEQ_ENDSEQ = [SEQ, ENDSEQ]
Q_SEQ_CONSEQ_ENDSEQ = [SEQ, CONSEQ, ENDSEQ]
Q_SEQ_MINSEQ = [SEQ, MINSEQ]
Q_SEQ_MAXSEQ = [SEQ, MAXSEQ]
Q_SEQ_MINSEQ_MAXSEQ = [SEQ, MINSEQ, MAXSEQ]
Q_SEQ_CONSEQ_MINSEQ = [SEQ, CONSEQ, MINSEQ]
Q_SEQ_CONSEQ_MAXSEQ = [SEQ, CONSEQ, MAXSEQ]
Q_SEQ_CONSEQ_MINSEQ_MAXSEQ = [SEQ, CONSEQ, MINSEQ, MAXSEQ]
Q_SEQ_ENDSEQ_MINSEQ = [SEQ, ENDSEQ, MINSEQ]
Q_SEQ_ENDSEQ_MAXSEQ = [SEQ, ENDSEQ, MAXSEQ]
Q_SEQ_ENDSEQ_MINSEQ_MAXSEQ = [SEQ, ENDSEQ, MINSEQ, MAXSEQ]
Q_SEQ_CONSEQ_ENDSEQ_MINSEQ = [SEQ, CONSEQ, ENDSEQ, MINSEQ]
Q_SEQ_CONSEQ_ENDSEQ_MAXSEQ = [SEQ, CONSEQ, ENDSEQ, MAXSEQ]
Q_SEQ_CONSEQ_ENDSEQ_MINSEQ_MAXSEQ = [SEQ, CONSEQ, ENDSEQ, MINSEQ, MAXSEQ]
Q_STATS_LIST = [Q_SEQ, Q_SEQ_CONSEQ,
                Q_SEQ_CONSEQ_ENDSEQ,
                Q_SEQ_CONSEQ_ENDSEQ_MINSEQ_MAXSEQ]

# =============================================================================
# statistics
# =============================================================================
STATS_IN = 'in'
STATS_IN_MIN = 'in_min'
STATS_IN_MAX = 'in_max'
STATS_IN_AVG = 'in_avg'
STATS_COMP = 'comp'
STATS_OUT = 'out'
STATS_OUT_MIN = 'out_min'
STATS_OUT_MAX = 'out_max'
STATS_OUT_AVG = 'out_avg'
STATS_ATT_LIST = [STATS_IN, STATS_IN_MIN, STATS_IN_MAX, STATS_IN_AVG,
                  STATS_COMP, STATS_OUT, STATS_OUT_MIN, STATS_OUT_MAX,
                  STATS_OUT_AVG]
# =============================================================================


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
    for alg in configuration[ALGORITHM_LIST]:
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


def gen_stats_experiment_list(configuration):
    '''
    Generate the list of statistical experiments
    '''
    exp_list = []
    parameter_conf = configuration[PARAMETER]
    # For every algorithm
    for op_list in configuration[OPERATOR_LIST]:
        # Default parameters configuration
        def_conf = get_default_experiment(parameter_conf)
        par_list = get_variated_parameters(configuration)
        if MINSEQ not in op_list:
            if MIN in par_list:
                par_list.remove(MIN)
            if MIN in def_conf:
                del def_conf[MIN]
        if MAXSEQ not in op_list:
            if MAX in par_list:
                par_list.remove(MAX)
            if MAX in def_conf:
                del def_conf[MAX]
        for par in par_list:
            # For every value in the variation
            for value in parameter_conf[par][VAR]:
                # Copy default values
                conf = def_conf.copy()
                # Set algorithm
                conf[OPERATOR_LIST] = op_list
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
        if par in experiment_conf:
            id_str += par + str(experiment_conf[par])
    return id_str


def get_max_data_timestamp(parameter_conf):
    '''
    Return the maximum timstamp for a generated data stream
    '''
    if VAR in parameter_conf[SLI]:
        return max(parameter_conf[RAN][VAR]) + max(parameter_conf[SLI][VAR])
    else:
        return max(parameter_conf[RAN][VAR]) + parameter_conf[SLI][DEF]


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
    par_list = [par for par in parameter_conf]
    return {par: parameter_conf[par][DEF] for par in par_list}


def get_variated_parameters(configuration):
    '''
    Return the list of parameters having variation
    '''
    par_list = []
    par_conf = configuration[PARAMETER]
    for par in par_conf:
        if VAR in par_conf[par]:
            par_list.append(par)
    par_list.sort()
    return par_list


def get_stats_id(configuration, experiment_conf):
    '''
    Return experiment identifier with statistics output
    '''
    id_str = ''
    par_list = get_variated_parameters(configuration)
    if MINSEQ not in experiment_conf[OPERATOR_LIST] \
            and MIN in par_list:
        par_list.remove(MIN)
    if MAXSEQ not in experiment_conf[OPERATOR_LIST] \
            and MAX in par_list:
        par_list.remove(MAX)
    # For every parameter
    for par in par_list:
        # Get value for this parameter in the current experiment
        id_str += par + str(experiment_conf[par])
    id_str = ':'.join(experiment_conf[OPERATOR_LIST]) + '_' + id_str
    return id_str
