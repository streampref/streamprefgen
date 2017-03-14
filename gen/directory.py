# -*- coding: utf-8 -*-
'''
Directories
'''

import csv
import os

from gen.experiment import ALG, get_id


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
# Directories and filenames for datasets to evaluate SEQ operator
# =============================================================================
SEQ_MAIN_DIR = 'streampref_seq'
SEQ_DIR_DICT = {dire: SEQ_MAIN_DIR + os.sep + dire
                for dire in DIR_LIST}
SEQ_DIR_DICT[MAIN_DIR] = SEQ_MAIN_DIR

# =============================================================================
# Directories and filenames for datasets to evaluate SEQ operator
# =============================================================================
TPREF_MAIN_DIR = 'streampref_tpref'
TPREF_DIR_DICT = {dire: TPREF_MAIN_DIR + os.sep + dire
                  for dire in DIR_LIST}
TPREF_DIR_DICT[MAIN_DIR] = TPREF_MAIN_DIR


def _create_directory(directory):
    '''
    Create a directory if it does not exists
    '''
    if not os.path.exists(directory):
        os.mkdir(directory)


def create_directories(experiment_list, parameter_conf, directory_dict,
                       algorithm_list):
    '''
    Create default directories if they do not exists
    '''
    # Create main directory
    _create_directory(directory_dict[MAIN_DIR])
    # Create remaining directories
    for directory in directory_dict.values():
        _create_directory(directory)
    # Create detail, output and environment directories for every algorithm
    for alg in algorithm_list:
        directory = directory_dict[ENV_DIR] + os.sep + alg
        _create_directory(directory)
        directory = directory_dict[OUT_DIR] + os.sep + alg
        _create_directory(directory)
        directory = directory_dict[DETAIL_DIR] + os.sep + alg
        _create_directory(directory)
    # Create query directories for every experiment
    for exp in experiment_list:
        exp_id = get_id(exp, parameter_conf)
        directory = directory_dict[QUERY_DIR] + os.sep + exp_id
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


def get_out_file(experiment_conf, parameter_conf, directory_dict):
    '''
    Return the correspondent output filename
    '''
    return directory_dict[OUT_DIR] + os.sep + experiment_conf[ALG] + os.sep + \
        get_id(experiment_conf, parameter_conf) + '.csv'


def get_data_file(experiment_conf, parameter_conf, directory_dict):
    '''
    Return the correspondent output filename
    '''
    return directory_dict[OUT_DIR] + os.sep + \
        get_id(experiment_conf, parameter_conf) + '.csv'


def get_query_dir(experiment_conf, parameter_conf, directory_dict):
    '''
    Return the correspondent query directory
    '''
    return directory_dict[QUERY_DIR] + os.sep + \
        experiment_conf[ALG] + os.sep + \
        get_id(experiment_conf, parameter_conf)


def get_detail_file(experiment_conf, parameter_conf, directory_dict, count):
    '''
    Return detail filename
    '''
    return directory_dict[DETAIL_DIR] + os.sep + experiment_conf[ALG] + \
        os.sep + get_id(experiment_conf, parameter_conf) + ':' + str(count) + \
        '.csv'


def get_env_file(experiment_conf, parameter_conf, directory_dict):
    '''
    Return detail filename
    '''
    return directory_dict[ENV_DIR] + os.sep + experiment_conf[ALG] + \
        os.sep + get_id(experiment_conf, parameter_conf) + '.env'


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
