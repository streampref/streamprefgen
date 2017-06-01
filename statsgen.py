#!/usr/bin/python -u
# -*- coding: utf-8 -*-

'''
Module for statistical experiments with operators
'''
from gen.experiment import RAN, VAR, DEF, MIN, MAX, DIRECTORY, PARAMETER,\
    OPERATOR_LIST, Q_STATS_LIST, gen_stats_experiment_list, ATT, NSQ, SLI, \
    RUL, LEV, IND
from gen.directory import STATS_DIR_DICT, create_stats_directories
from gen.data import gen_all_streams
from gen.query.stats import gen_all_queries, gen_all_env
from gen.run import run_stats_experiments, summarize_all_stats


# =============================================================================
# Experiment execution
# =============================================================================
# Match count
MATCH_COUNT = 1

# Parameters configuration
STATS_PAR = {
    # Attributes
    ATT: {
        #  VAR: [8, 10, 12, 14, 16],
        DEF: 8
        },
    # Sequences
    NSQ: {
        VAR: [4, 8, 16, 24, 32],
        DEF: 16
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
        #  VAR: [1, 2, 3, 4, 5, 6],
        DEF: 5
        },
    # Min
    MIN: {
        VAR: [5, 10, 20, 40],
        DEF: 5
        },
    # Max
    MAX: {
        VAR: [5, 10, 20, 40],
        DEF: 40
        }
    }

STATS_CONF = {
    # Algorithms
    OPERATOR_LIST: Q_STATS_LIST,
    # Main directory
    DIRECTORY: STATS_DIR_DICT,
    # Parameters
    PARAMETER: STATS_PAR
    }


def get_arguments(print_help=False):
    '''
    Get arguments
    '''
    import argparse
    parser = argparse.ArgumentParser('STATSGen')
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
    exp_list = gen_stats_experiment_list(STATS_CONF)
    if args.gen:
        create_stats_directories(STATS_CONF)
        print 'Generating stream data'
        gen_all_streams(STATS_CONF, exp_list)
        print 'Generating queries'
        gen_all_queries(STATS_CONF, exp_list)
        print 'Generating environments'
        gen_all_env(STATS_CONF, exp_list)
    elif args.run:
        print 'Running experiments'
        run_stats_experiments(STATS_CONF, exp_list)
    elif args.summarize:
        print 'Summarizing results'
        summarize_all_stats(STATS_CONF)
    else:
        get_arguments(True)


if __name__ == '__main__':
    main()
