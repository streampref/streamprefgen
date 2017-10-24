#!/usr/bin/python -u
# -*- coding: utf-8 -*-
'''
Dataset generator for experiments with temporal preference operators
'''

from gen.data import gen_all_streams
from gen.directory import BESTSEQ_DIR_DICT, create_directories
from gen.experiment import ATT, VAR, DEF, NSQ, RAN, SLI, RUL, LEV, \
    IND, DIRECTORY, PARAMETER, CQL_ALG, BNL_SEARCH_ALG, \
    INC_PARTITION_SEQTREE_ALG, INC_PARTITIONLIST_SEQTREE_ALG, \
    INC_PARTITION_SEQTREE_PRUNING_ALG, INC_PARTITIONLIST_SEQTREE_PRUNING_ALG, \
    ALGORITHM_LIST, MAX_VALUE, TUPLE_RATE, gen_experiment_list
from gen.query.bestseq import gen_all_queries, gen_all_env
from gen.run import run_experiments, summarize_all, confidence_interval_all


# Parameters configuration
BESTSEQ_PAR = {
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
        VAR: [1, 2, 3, 4, 5, 6],
        DEF: 2
        }
    }

BESTSEQ_CONF = {
    # Algorithms
    ALGORITHM_LIST: [CQL_ALG, BNL_SEARCH_ALG, INC_PARTITION_SEQTREE_ALG,
                     INC_PARTITIONLIST_SEQTREE_ALG,
                     INC_PARTITION_SEQTREE_PRUNING_ALG,
                     INC_PARTITIONLIST_SEQTREE_PRUNING_ALG],
    # Directories
    DIRECTORY: BESTSEQ_DIR_DICT,
    # Parameters
    PARAMETER: BESTSEQ_PAR,
    # Maximum attribute value
    MAX_VALUE: 32,
    # Percent of sequence identifier per instant
    TUPLE_RATE: 0.75
    }

# Number of executions for experiments
RUN_COUNT = 2


def get_arguments(print_help=False):
    '''
    Get arguments
    '''
    import argparse
    parser = argparse.ArgumentParser('BESTSEQGen')
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
    exp_list = gen_experiment_list(BESTSEQ_CONF)
    if args.gen:
        create_directories(BESTSEQ_CONF, exp_list)
        print 'Generating stream data'
        gen_all_streams(BESTSEQ_CONF, exp_list)
        print 'Generating queries'
        gen_all_queries(BESTSEQ_CONF, exp_list)
        print 'Generating environments'
        gen_all_env(BESTSEQ_CONF, exp_list, output=args.output)
    elif args.run:
        print 'Running experiments'
        run_experiments(BESTSEQ_CONF, exp_list, RUN_COUNT)
    elif args.summarize:
        print 'Summarizing results'
        summarize_all(BESTSEQ_CONF, RUN_COUNT)
        print 'Calculating confidence intervals'
        confidence_interval_all(BESTSEQ_CONF)
    else:
        get_arguments(True)


if __name__ == '__main__':
    main()
