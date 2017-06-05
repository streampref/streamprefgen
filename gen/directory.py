# -*- coding: utf-8 -*-
'''
Directories
'''

import csv
import os

from gen.experiment import DIRECTORY, PARAMETER, ALGORITHM_LIST, ALGORITHM, \
    get_id, get_data_id, get_stats_id


# =============================================================================
# Generic directories and files
# =============================================================================
MAIN_DIR = 'main'
DATA_DIR = 'data'
QUERY_DIR = 'queries'
ENV_DIR = 'env'
OUT_DIR = 'out'
DETAIL_DIR = 'details'
SUMMARY_DIR = 'summary'
RESULT_DIR = 'result'
DIR_LIST = [MAIN_DIR, DATA_DIR, QUERY_DIR, ENV_DIR, OUT_DIR, DETAIL_DIR,
            SUMMARY_DIR, RESULT_DIR]

# =============================================================================
# Directory configuration for experiments with SEQ operator
# =============================================================================
SEQ_MAIN_DIR = 'exp_seq'
SEQ_DIR_DICT = {dire: SEQ_MAIN_DIR + os.sep + dire
                for dire in DIR_LIST}
SEQ_DIR_DICT[MAIN_DIR] = SEQ_MAIN_DIR

# =============================================================================
# Directory configuration for experiments with temporal preference operators
# =============================================================================
BESTSEQ_MAIN_DIR = 'exp_bestseq'
BESTSEQ_DIR_DICT = {dire: BESTSEQ_MAIN_DIR + os.sep + dire
                    for dire in DIR_LIST}
BESTSEQ_DIR_DICT[MAIN_DIR] = BESTSEQ_MAIN_DIR

# =============================================================================
# Directory configuration for experiments with CONSEQ operator
# =============================================================================
CONSEQ_MAIN_DIR = 'exp_conseq'
CONSEQ_DIR_DICT = {dire: CONSEQ_MAIN_DIR + os.sep + dire
                   for dire in DIR_LIST}
CONSEQ_DIR_DICT[MAIN_DIR] = CONSEQ_MAIN_DIR

# =============================================================================
# Directory configuration for experiments with ENDSEQ operator
# =============================================================================
ENDSEQ_MAIN_DIR = 'exp_endseq'
ENDSEQ_DIR_DICT = {dire: ENDSEQ_MAIN_DIR + os.sep + dire
                   for dire in DIR_LIST}
ENDSEQ_DIR_DICT[MAIN_DIR] = ENDSEQ_MAIN_DIR

# =============================================================================
# Directory configuration for experiments with MINSEQ operator
# =============================================================================
MINSEQ_MAIN_DIR = 'exp_minseq'
MINSEQ_DIR_DICT = {dire: MINSEQ_MAIN_DIR + os.sep + dire
                   for dire in DIR_LIST}
MINSEQ_DIR_DICT[MAIN_DIR] = MINSEQ_MAIN_DIR

# =============================================================================
# Directory configuration for experiments with MAXSEQ operator
# =============================================================================
MAXSEQ_MAIN_DIR = 'exp_maxseq'
MAXSEQ_DIR_DICT = {dire: MAXSEQ_MAIN_DIR + os.sep + dire
                   for dire in DIR_LIST}
MAXSEQ_DIR_DICT[MAIN_DIR] = MAXSEQ_MAIN_DIR

# =============================================================================
# Directories for statistical experiments
# =============================================================================
STATS_MAIN_DIR = 'exp_stats'
STATS_DIR_DICT = {dire: STATS_MAIN_DIR + os.sep + dire
                  for dire in DIR_LIST}
STATS_DIR_DICT[MAIN_DIR] = STATS_MAIN_DIR
# =============================================================================


def _create_directory(directory):
    '''
    Create a directory if it does not exists
    '''
    if not os.path.exists(directory):
        os.mkdir(directory)


def create_directories(configuration, experiment_list):
    '''
    Create default directories if they do not exists
    '''
    dir_dict = configuration[DIRECTORY]
    # Create main directory
    _create_directory(dir_dict[MAIN_DIR])
    # Create remaining directories
    for directory in dir_dict.values():
        _create_directory(directory)
    # Create detail, output and environment directories for every algorithm
    for alg in configuration[ALGORITHM_LIST]:
        directory = dir_dict[ENV_DIR] + os.sep + alg
        _create_directory(directory)
        directory = dir_dict[OUT_DIR] + os.sep + alg
        _create_directory(directory)
        directory = dir_dict[DETAIL_DIR] + os.sep + alg
        _create_directory(directory)
    # Create query directories for every experiment
    for alg in configuration[ALGORITHM_LIST]:
        directory = dir_dict[QUERY_DIR] + os.sep + alg
        _create_directory(directory)
        for exp in experiment_list:
            exp_id = get_id(exp, configuration[PARAMETER])
            directory = dir_dict[QUERY_DIR] + os.sep + alg + os.sep + exp_id
            _create_directory(directory)


def create_stats_directories(configuration):
    '''
    Create directories for statistical experiments
    '''
    dir_dict = configuration[DIRECTORY]
    # Create main directory
    _create_directory(dir_dict[MAIN_DIR])
    # Create remaining directories
    for directory in dir_dict.values():
        _create_directory(directory)


def write_to_csv(filename, attribute_list, record_list):
    '''
    Store record list into a CSV file
    '''
    # Check if file does not exists
    if not os.path.isfile(filename):
        # Store data to file
        data_file = open(filename, 'w')
        writer = csv.DictWriter(data_file, attribute_list, delimiter=',')
        writer.writeheader()
        writer.writerows(record_list)
        data_file.close()


def write_to_txt(filename, text):
    '''
    Store record list into a CSV file
    '''
    # Check if file does not exists
    if not os.path.isfile(filename):
        # Store data to file
        out_file = open(filename, 'w')
        out_file.write(text)
        out_file.close()


def get_out_file(configuration, experiment_conf):
    '''
    Return the correspondent output filename
    '''
    dir_dict = configuration[DIRECTORY]
    return dir_dict[OUT_DIR] + os.sep + experiment_conf[ALGORITHM] + os.sep + \
        get_id(experiment_conf, configuration[PARAMETER]) + '.csv'


def get_data_file(configuration, experiment_conf):
    '''
    Return the correspondent output filename
    '''
    dir_dict = configuration[DIRECTORY]
    return dir_dict[DATA_DIR] + os.sep + get_data_id(experiment_conf) + '.csv'


def get_query_dir(configuration, experiment_conf):
    '''
    Return the correspondent query directory
    '''
    dir_dict = configuration[DIRECTORY]
    return dir_dict[QUERY_DIR] + os.sep + \
        experiment_conf[ALGORITHM] + os.sep + \
        get_id(experiment_conf, configuration[PARAMETER])


def get_detail_file(configuration, experiment_conf, count):
    '''
    Return detail filename
    '''
    dir_dict = configuration[DIRECTORY]
    return dir_dict[DETAIL_DIR] + os.sep + experiment_conf[ALGORITHM] + \
        os.sep + get_id(experiment_conf, configuration[PARAMETER]) + ':' + \
        str(count) + '.csv'


def get_env_file(configuration, experiment_conf):
    '''
    Return detail filename
    '''
    dir_dict = configuration[DIRECTORY]
    return dir_dict[ENV_DIR] + os.sep + experiment_conf[ALGORITHM] + \
        os.sep + get_id(experiment_conf, configuration[PARAMETER]) + '.env'


def get_summary_file(configuration, summary, parameter):
    '''
    Return summary filename
    '''
    dir_dict = configuration[DIRECTORY]
    if summary != '':
        summary += '_'
    return dir_dict[SUMMARY_DIR] + os.sep + summary + parameter + '.csv'


def get_result_file(configuration, summary, parameter):
    '''
    Return result filename
    '''
    dir_dict = configuration[DIRECTORY]
    return dir_dict[RESULT_DIR] + os.sep + summary + '_' + parameter + '.csv'


def get_tup_file(configuration):
    '''
    Return tup filename
    '''
    dir_dict = configuration[DIRECTORY]
    return dir_dict[DATA_DIR] + os.sep + 'tup.csv'


def get_query_stats_file(configuration, experiment_conf):
    '''
    Return query filename for statistics query
    '''
    exp_id = get_stats_id(configuration, experiment_conf)
    return configuration[DIRECTORY][QUERY_DIR] + os.sep + exp_id + '.cql'


def get_env_stats_file(configuration, experiment_conf):
    '''
    Return environment filename for statistics query
    '''
    exp_id = get_stats_id(configuration, experiment_conf)
    return configuration[DIRECTORY][ENV_DIR] + os.sep + exp_id + '.env'


def get_detail_stats_file(configuration, experiment_conf):
    '''
    Return statistics detail file
    '''
    exp_id = get_stats_id(configuration, experiment_conf)
    return configuration[DIRECTORY][DETAIL_DIR] + os.sep + exp_id + '.csv'


def write_result_file(filename, record_list, key_field):
    '''
    Write to a result file
    '''
    # Check if there exists records to be stored
    if len(record_list):
        # Get the field list
        field_list = [field for field in record_list[0].keys()
                      if field != key_field]
        # Sort the field list
        field_list.sort()
        # Put key field in the beginning of field list
        field_list.insert(0, key_field)
        write_to_csv(filename, field_list, record_list)
