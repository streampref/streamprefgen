#!/usr/bin/python -u
# -*- coding: utf-8 -*-
'''
Dataset generator for experiments with SEQ operator
'''

from gen.directory import SEQ_DIR_DICT, create_directories
from gen.experiment import ATT, NSQ, IDA, RAN, SLI, VAR, DEF, PSI, \
    SEQ_ALGORITHM_LIST, gen_experiment_list
from gen.run import run_experiments, summarize_all, confidence_interval_all
from gen.query.seq import gen_all_queries, gen_all_env_files
from gen.streamgen import gen_all_streams


# Parameters configuration
SEQ_PAR_CONF = {
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
    # Percent of sequence per instant
    PSI: {
        DEF: 0.5
        },
    # Identifier attributes
    IDA: {
        DEF: 1
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
        }
    }

# Number of executions for experiments
RUN_COUNT = 2


def get_arguments(print_help=False):
    '''
    Get arguments
    '''
    import argparse
    parser = argparse.ArgumentParser('SeqGen')
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
    exp_list = gen_experiment_list(SEQ_PAR_CONF, SEQ_ALGORITHM_LIST)
    if args.gen:
        create_directories(SEQ_DIR_DICT, SEQ_ALGORITHM_LIST)
        print 'Generating stream data'
        gen_all_streams(exp_list, SEQ_PAR_CONF, SEQ_DIR_DICT)
        print 'Generating queries'
        gen_all_queries(exp_list, SEQ_PAR_CONF, SEQ_DIR_DICT)
        print 'Generating environments'
        if args.output:
            gen_all_env_files(exp_list, SEQ_PAR_CONF, SEQ_DIR_DICT,
                              output=True)
        else:
            gen_all_env_files(exp_list, SEQ_PAR_CONF, SEQ_DIR_DICT)
    elif args.run:
        print 'Running experiments'
        run_experiments(exp_list, SEQ_PAR_CONF, SEQ_DIR_DICT, RUN_COUNT)
    elif args.summarize:
        print 'Summarizing results'
        summarize_all(SEQ_PAR_CONF, SEQ_DIR_DICT, SEQ_ALGORITHM_LIST,
                      RUN_COUNT)
        print 'Calculating confidence intervals'
        confidence_interval_all(SEQ_PAR_CONF, SEQ_DIR_DICT)
    else:
        get_arguments(True)


if __name__ == '__main__':
    main()
