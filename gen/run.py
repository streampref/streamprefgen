# -*- coding: utf-8 -*-
'''
Experiments execution
'''

import csv
import os

from gen.directory import get_detail_file, get_env_file, write_result_file, \
    get_summary_file, get_result_file
from gen.experiment import PARAMETER, RAN, VAR, SLI, CQL_ALG, \
    SEQ_ALG, RUNTIME, MEMORY, SUM_RUN, SUM_MEM, BNL_SEARCH, \
    INC_PARTITION_SEQTREE_ALG, INC_PARTITIONLIST_SEQTREE_ALG, \
    INC_PARTITION_SEQTREE_PRUNING_ALG, INC_PARTITIONLIST_SEQTREE_PRUNING_ALG, \
    ALGORITHM, ALGORITHM_LIST, get_varied_parameters, get_default_experiment,\
    NAIVE_SUBSEQ, INC_SUBSEQ


# Command for experiment run (without parameters for algorithms)
SIMPLE_RUN_COMMAND = "streampref -e {env} -d {det} -m {max}"
# Command for experiment run with temporal preference algorithm option
TPREF_RUN_COMMAND = "streampref -e {env} -d {det} -m {max} -t {alg}"
# Command for experiment run with subsequence algorithm option
SUBSEQ_RUN_COMMAND = "streampref -e {env} -d {det} -m {max} -s {alg}"
# Command for calculation of confidence interval
CONFINTERVAL_COMMAND = "confinterval -i {inf} -o {outf} -k {keyf}"

# Dictionary of run commands
RUN_DICT = {}
# CQL run command
RUN_DICT[CQL_ALG] = SIMPLE_RUN_COMMAND
# SEQ run command
RUN_DICT[SEQ_ALG] = SIMPLE_RUN_COMMAND
# Temporal preference run commands
RUN_DICT[BNL_SEARCH] = SIMPLE_RUN_COMMAND
RUN_DICT[INC_PARTITION_SEQTREE_ALG] = TPREF_RUN_COMMAND
RUN_DICT[INC_PARTITIONLIST_SEQTREE_ALG] = TPREF_RUN_COMMAND
RUN_DICT[INC_PARTITION_SEQTREE_PRUNING_ALG] = TPREF_RUN_COMMAND
RUN_DICT[INC_PARTITIONLIST_SEQTREE_PRUNING_ALG] = TPREF_RUN_COMMAND
# Subsequence run commands
RUN_DICT[NAIVE_SUBSEQ] = SUBSEQ_RUN_COMMAND
RUN_DICT[INC_SUBSEQ] = SUBSEQ_RUN_COMMAND


def run(configuration, experiment_conf, count):
    '''
    Run an experiment
    '''
    parameter_conf = configuration[PARAMETER]
    # Get iteration number
    iterations = experiment_conf[RAN] + max(parameter_conf[SLI][VAR])
    # Get environment file
    env_file = get_env_file(configuration, experiment_conf)
    # Get detail file
    detail_file = get_detail_file(configuration, experiment_conf, count)
    if not os.path.isfile(detail_file):
        command = RUN_DICT[experiment_conf[ALGORITHM]]
        if command == SIMPLE_RUN_COMMAND:
            command = command.format(env=env_file, det=detail_file,
                                     max=iterations)
        elif command in [TPREF_RUN_COMMAND, SUBSEQ_RUN_COMMAND]:
            command = command.format(env=env_file, det=detail_file,
                                     max=iterations,
                                     alg=experiment_conf[ALGORITHM])
        print command
        os.system(command)
        if not os.path.isfile(detail_file):
            print 'Detail results file not found: ' + detail_file
            print "Check if 'streampref' is in path"


def run_experiments(configuration, experiment_list, run_count):
    '''
    Run all experiments
    '''
    for count in range(1, run_count + 1):
        for exp_conf in experiment_list:
            run(configuration, exp_conf, count)


def get_summaries(detail_file):
    '''
    Read results from a detail file
    '''
    # Check if file exists
    if not os.path.isfile(detail_file):
        print 'File does not exists: ' + detail_file
        return (float('NaN'), float('NaN'))
    in_file = open(detail_file, 'r')
    reader = csv.DictReader(in_file, skipinitialspace=True)
    sum_time = 0.0
    sum_memory = 0.0
    count = 0
    for rec in reader:
        sum_time += float(rec[RUNTIME])
        sum_memory += float(rec[MEMORY])
        count += 1
    in_file.close()
    # Return total runtime and memory average
    return (sum_time, sum_memory / count)


def summarize(configuration, parameter, run_count):
    '''
    Summarize experiments about range variation
    '''
    # Result lists
    time_list = []
    mem_list = []
    # Get parameter configurations
    par_conf = configuration[PARAMETER]
    # Get default parameter values
    exp_conf = get_default_experiment(par_conf)
    # For every value of current attributes
    for value in par_conf[parameter][VAR]:
        # Get experiment configuration for current value
        exp_conf[parameter] = value
        # For every execution
        for count in range(1, run_count + 1):
            # Creates record for current parameter and value
            time_rec = {parameter: value}
            mem_rec = {parameter: value}
            # For every algorithm
            for alg in configuration[ALGORITHM_LIST]:
                exp_conf[ALGORITHM] = alg
                # Get detail file
                filename = get_detail_file(configuration, exp_conf, count)
                # Get summarized results
                runtime, memory = get_summaries(filename)
                time_rec[alg] = runtime
                mem_rec[alg] = memory
            # Append to result lists
            time_list.append(time_rec)
            mem_list.append(mem_rec)
    # Store summarized results
    filename = get_summary_file(configuration, SUM_RUN, parameter)
    write_result_file(filename, time_list, parameter)
    filename = get_summary_file(configuration, SUM_MEM, parameter)
    write_result_file(filename, mem_list, parameter)


def summarize_all(configuration, run_count):
    '''
    Summarize all results
    '''
    # Get parameter having variation
    par_list = get_varied_parameters(configuration[PARAMETER])
    for par in par_list:
        summarize(configuration, par, run_count)


def confidence_interval(parameter, in_file, out_file):
    '''
    Calculate final result with confidence interval
    '''
    if not os.path.isfile(in_file):
        print 'File does not exists: ' + in_file
        return
    command = CONFINTERVAL_COMMAND.format(inf=in_file, outf=out_file,
                                          keyf=parameter)
    print command
    os.system(command)
    if not os.path.isfile(out_file):
        print 'Output file not found: ' + out_file
        print "Check if 'confinterval' is in path"


def confidence_interval_all(configuration):
    '''
    Calculate confidence interval for all summarized results
    '''
    par_list = get_varied_parameters(configuration[PARAMETER])
    # For every parameter
    for parameter in par_list:
        in_file = get_summary_file(configuration, SUM_RUN, parameter)
        out_file = get_result_file(configuration, SUM_RUN, parameter)
        confidence_interval(parameter, in_file, out_file)
        in_file = get_summary_file(configuration, SUM_MEM, parameter)
        out_file = get_result_file(configuration, SUM_MEM, parameter)
        confidence_interval(parameter, in_file, out_file)
