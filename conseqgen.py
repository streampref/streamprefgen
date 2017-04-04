#!/usr/bin/python -u
# -*- coding: utf-8 -*-
'''
Dataset generator for experiments with CONSEQ operator
'''

from gen.directory import CONSEQ_DIR_DICT, create_directories
from gen.experiment import ATT, VAR, DEF, NSQ, RAN, SLI, \
    PARAMETER, DIRECTORY, NAIVE_SUBSEQ, INC_SUBSEQ, \
    gen_experiment_list, PCT, ALGORITHM_LIST, CQL_ALG
from gen.query.conseq import gen_all_queries, gen_all_env
from gen.run import run_experiments, summarize_all, confidence_interval_all
from gen.streamgen import gen_all_conseq_streams


# Parameters configuration
CONSEQ_PAR = {
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
    # Percent of consecutive timestamps
    PCT: {
        VAR: [0.0, 0.25, 0.5, 0.75, 1.0],
        DEF: 0.5
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
    }

CONSEQ_CONF = {
    # Algorithms
    ALGORITHM_LIST: [CQL_ALG, NAIVE_SUBSEQ, INC_SUBSEQ],
    # Directories
    DIRECTORY: CONSEQ_DIR_DICT,
    # Parameters
    PARAMETER: CONSEQ_PAR
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
    exp_list = gen_experiment_list(CONSEQ_CONF)
    if args.gen:
        create_directories(CONSEQ_CONF, exp_list)
        print 'Generating stream data'
        gen_all_conseq_streams(CONSEQ_CONF, exp_list)
        print 'Generating queries'
        gen_all_queries(CONSEQ_CONF, exp_list)
        print 'Generating environments'
        gen_all_env(CONSEQ_CONF, exp_list, output=args.output)
    elif args.run:
        print 'Running experiments'
        run_experiments(CONSEQ_CONF, exp_list, RUN_COUNT)
    elif args.summarize:
        print 'Summarizing results'
        summarize_all(CONSEQ_CONF, RUN_COUNT)
        print 'Calculating confidence intervals'
        confidence_interval_all(CONSEQ_CONF)
    else:
        get_arguments(True)


if __name__ == '__main__':
    main()
