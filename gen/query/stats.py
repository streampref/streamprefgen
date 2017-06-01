# -*- coding: utf-8 -*-
'''
Queries for experiments with statistics operators
'''
from gen.experiment import OPERATOR_LIST, ENDSEQ, CONSEQ, RAN, SLI, MINSEQ, MIN,\
    MAXSEQ, MAX
from gen.query.rule import gen_rules_dict, get_temporal_preferences
from gen.directory import get_query_stats_file, write_to_txt, get_env_stats_file
from gen.query.basic import get_register_stream, REG_Q_STR


# =============================================================================
# Queries with preference operators
# =============================================================================
Q_SEQ = '''
SEQUENCE IDENTIFIED BY a1 [RANGE {ran} SECOND, SLIDE {sli} SECOND] FROM s\n
'''

Q_CONSEQ = '''
SUBSEQUENCE CONSECUTIVE TIMESTAMP FROM\n
'''

Q_ENDSEQ = '''
SUBSEQUENCE END POSITION FROM\n
'''

Q_MINSEQ = '''
MINIMUM LENGTH IS {min}
'''

Q_MAXSEQ = '''
MAXIMUM LENGTH IS {max}
'''

Q_BESTSEQ = '''
TEMPORAL PREFERENCES
{pref}
;
'''


def gen_stats_query(configuration, experiment_conf):
    '''
    Generate single query
    '''
    op_list = experiment_conf[OPERATOR_LIST]
    query = 'SELECT '
    # ENDSEQ
    if ENDSEQ in op_list:
        query += Q_ENDSEQ
    # CONSEQ
    if CONSEQ in op_list:
        query += Q_CONSEQ
    # SEQ
    query += Q_SEQ.format(ran=experiment_conf[RAN],
                          sli=experiment_conf[SLI])
    where_list = []
    # MINSEQ
    if MINSEQ in op_list:
        where_list.append(Q_MINSEQ.format(min=experiment_conf[MIN]))
    # MAXSEQ
    if MAXSEQ in op_list:
        where_list.append(Q_MAXSEQ.format(max=experiment_conf[MAX]))
    if len(where_list):
        query += '\nWHERE ' + ' AND '.join(where_list)
    # Select correct query
    rules_dict = gen_rules_dict(experiment_conf)
    pref_str = get_temporal_preferences(rules_dict)
    query += Q_BESTSEQ.format(pref=pref_str)
    # Store query code
    filename = get_query_stats_file(configuration, experiment_conf)
    write_to_txt(filename, query)


def gen_all_queries(configuration, experiment_list):
    '''
    Generate all queries
    '''
    # For every experiment
    for exp_conf in experiment_list:
        # Generate appropriate queries
        gen_stats_query(configuration, exp_conf)


def gen_stats_env(configuration, experiment_conf):
    '''
    Generate environment
    '''
    text = get_register_stream(configuration, experiment_conf)
    # Get query filename
    filename = get_query_stats_file(configuration, experiment_conf)
    # Register query
    text += REG_Q_STR.format(qname='stats', qfile=filename)
    # Get environment filename
    filename = get_env_stats_file(configuration, experiment_conf)
    write_to_txt(filename, text)


def gen_all_env(configuration, experiment_list):
    '''
    Generate all environments
    '''
    for exp_conf in experiment_list:
        gen_stats_env(configuration, exp_conf)
