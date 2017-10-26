#!/usr/bin/python -u
# -*- coding: utf-8 -*-

'''
Module for utility experiments with operators
'''
from gen.data import gen_all_streams
from gen.directory import UTIL_DIR_DICT, create_util_directories
from gen.experiment import RAN, VAR, DEF, MIN, MAX, DIRECTORY, PARAMETER, \
    OPERATOR_LIST, Q_UTIL_LIST, ATT, NSQ, SLI, RUL, LEV, IND, MAX_VALUE, \
    TUPLE_RATE, gen_util_experiment_list
from gen.query.util import gen_all_queries, gen_all_env
from gen.run import run_util_experiments, summarize_all_util


# =============================================================================
# Experiment execution
# =============================================================================
# Match count
MATCH_COUNT = 1

# Parameters configuration
UTIL_PAR = {
    # Attributes
    ATT: {
        VAR: [5, 10, 15, 20, 25],
        DEF: 5
        },
    # Sequences
    NSQ: {
        VAR: [2, 4, 6, 8, 10],
        DEF: 8
        },
    # Range
    RAN: {
        VAR: [10, 20, 40, 60, 80, 100],
        DEF: 40
        },
    # Slide
    SLI: {
        VAR: [1, 10, 20, 30, 40],
        DEF: 1
        },
    # Rules
    RUL: {
        VAR: [4, 8, 16, 24, 32],
        DEF: 8
        },
    # Preference level
    LEV: {
        VAR: [1, 2, 3, 4, 5, 6],
        DEF: 2
        },
    # Indifferent attributes
    IND: {
        DEF: 2
        },
    # Min
    MIN: {
        VAR: [2, 4, 6, 8, 10],
        DEF: 2
        },
    # Max
    MAX: {
        VAR: [5, 10, 20, 30, 40],
        DEF: 40
        }
    }

UTIL_CONF = {
    # Algorithms
    OPERATOR_LIST: Q_UTIL_LIST,
    # Main directory
    DIRECTORY: UTIL_DIR_DICT,
    # Parameters
    PARAMETER: UTIL_PAR,
    # Maximum attribute value
    MAX_VALUE: 10,
    # Percent of sequence identifier per instant
    TUPLE_RATE: 0.75
    }


def get_arguments(print_help=False):
    '''
    Get arguments
    '''
    import argparse
    parser = argparse.ArgumentParser('UTILGen')
    parser.add_argument('-g', '--gen', action="store_true",
                        default=False,
                        help='Generate files')
    parser.add_argument('-r', '--run', action="store_true",
                        default=False,
                        help='Run experiments')
    parser.add_argument('-s', '--summarize', action="store_true",
                        default=False,
                        help='Summarize results')
    args = parser.parse_args()
    if print_help:
        parser.print_help()
    return args


def main():
    '''
    Main routine
    '''
    args = get_arguments()
    exp_list = gen_util_experiment_list(UTIL_CONF)
    if args.gen:
        create_util_directories(UTIL_CONF)
        print 'Generating stream data'
        gen_all_streams(UTIL_CONF, exp_list)
        print 'Generating queries'
        gen_all_queries(UTIL_CONF, exp_list)
        print 'Generating environments'
        gen_all_env(UTIL_CONF, exp_list)
    elif args.run:
        print 'Running experiments'
        run_util_experiments(UTIL_CONF, exp_list)
    elif args.summarize:
        print 'Summarizing results'
        summarize_all_util(UTIL_CONF)
    else:
        get_arguments(True)


if __name__ == '__main__':
    main()
