# -*- coding: utf-8 -*-
'''
Queries for experiments with temporal preference operators
'''

import os

from gen.directory import write_to_txt, get_env_file, \
    get_query_dir, get_out_file
from gen.experiment import RAN, SLI, TOP
from gen.query.basic import REG_Q_STR, \
    REG_Q_OUTPUT_STR, get_register_stream
from gen.query.rule import gen_rules_dict, \
    get_temporal_preferences


# =============================================================================
# StreamPref Query
# =============================================================================
TOPKSEQ_QUERY = '''
SELECT TOP({top}) SEQUENCE
IDENTIFIED BY a1 [RANGE {ran} SECOND, SLIDE {sli} SECOND]
FROM s TEMPORAL PREFERENCES
{pref};
'''


def gen_topkseq_query(configuration, experiment_conf):
    '''
    Generate StreamPref queries with TOPKSEQ operator
    '''
    filename = get_query_dir(configuration, experiment_conf) + \
        os.sep + 'topkseq.cql'
    rules_dict = gen_rules_dict(configuration, experiment_conf)
    pref_str = get_temporal_preferences(rules_dict)
    query = TOPKSEQ_QUERY.format(top=experiment_conf[TOP],
                                 ran=experiment_conf[RAN],
                                 sli=experiment_conf[SLI],
                                 pref=pref_str)
    write_to_txt(filename, query)


def gen_all_queries(configuration, experiment_list):
    '''
    Generate all queries
    '''
    for exp_conf in experiment_list:
        gen_topkseq_query(configuration, exp_conf)


def gen_topkseq_env(configuration, experiment_conf, output):
    '''
    Generate environment files StreamPref operator
    '''
    # Register stream
    text = get_register_stream(configuration, experiment_conf)
    # Get query filename
    filename = get_query_dir(configuration, experiment_conf) + \
        os.sep + 'topkseq.cql'
    # Register query
    if output:
        # Get output filename
        out_file = get_out_file(configuration, experiment_conf)
        text += REG_Q_OUTPUT_STR.format(qname='topkseq', qfile=filename,
                                        ofile=out_file)
    else:
        text += REG_Q_STR.format(qname='topkseq', qfile=filename)
    # Get environment filename
    filename = get_env_file(configuration, experiment_conf)
    write_to_txt(filename, text)


def gen_all_env(configuration, experiment_list, output=False):
    '''
    Generate all environment files
    '''
    for exp_conf in experiment_list:
        gen_topkseq_env(configuration, exp_conf, output)
