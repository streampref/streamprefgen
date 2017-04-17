#!/usr/bin/python -u
# -*- coding: utf-8 -*-
'''
Dataset generator for experiments with MINSEQ operator
'''
from gen.experiment import VAR, DEF, ATT, NSQ, RAN, SLI, MIN, ALGORITHM_LIST,\
    CQL_ALG, DIRECTORY, PARAMETER, FILTER, gen_experiment_list
from gen.directory import MINSEQ_DIR_DICT, create_directories
from gen.data import gen_all_streams
from gen.query.minseq import gen_all_queries, gen_all_env
from gen.run import run_experiments, summarize_all, confidence_interval_all


# Parameters configuration
MINSEQ_PAR = {
    # Attributes
    ATT: {
        VAR: [8, 10, 12, 14, 16],
        DEF: 10
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
        DEF: 10
        },
    # Minimum size of sequences
    MIN: {
        VAR: [5, 10, 15, 20, 25, 30, 35, 40],
        DEF: 10
        },
    }

MINSEQ_CONF = {
    # Algorithms
    ALGORITHM_LIST: [CQL_ALG, FILTER],
    # Directories
    DIRECTORY: MINSEQ_DIR_DICT,
    # Parameters
    PARAMETER: MINSEQ_PAR
    }

# Number of executions for experiments
RUN_COUNT = 2


def get_arguments(print_help=False):
    '''
    Get arguments
    '''
    import argparse
    parser = argparse.ArgumentParser('ConseqGen')
    parser.add_argument('-g', '--gen', action="store_true",
                        default=False,
                        help='Generate files')
    parser.add_argument('-o', '--output', action="store_true",
                        default=False,
                        help='Generate query output')
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
    exp_list = gen_experiment_list(MINSEQ_CONF)
    if args.gen:
        create_directories(MINSEQ_CONF, exp_list)
        print 'Generating stream data'
        gen_all_streams(MINSEQ_CONF, exp_list)
        print 'Generating queries'
        gen_all_queries(MINSEQ_CONF, exp_list)
        print 'Generating environments'
        gen_all_env(MINSEQ_CONF, exp_list, output=args.output)
    elif args.run:
        print 'Running experiments'
        run_experiments(MINSEQ_CONF, exp_list, RUN_COUNT)
    elif args.summarize:
        print 'Summarizing results'
        summarize_all(MINSEQ_CONF, RUN_COUNT)
        print 'Calculating confidence intervals'
        confidence_interval_all(MINSEQ_CONF)
    else:
        get_arguments(True)


if __name__ == '__main__':
    main()
