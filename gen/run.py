# -*- coding: utf-8 -*-
'''
Experiments execution
'''

import csv
import os

from gen.directory import get_detail_file, get_env_file, write_result_file, \
    get_summary_file, get_result_file, get_env_util_file, \
    get_detail_util_file
from gen.experiment import PARAMETER, RAN, VAR, SLI, CQL_ALG, \
    SEQ_ALG, RUNTIME, MEMORY, SUM_RUN, SUM_MEM, BNL_SEARCH_ALG, \
    INC_PARTITION_SEQTREE_ALG, INC_PARTITIONLIST_SEQTREE_ALG, \
    INC_PARTITION_SEQTREE_PRUNING_ALG, INC_PARTITIONLIST_SEQTREE_PRUNING_ALG, \
    ALGORITHM, ALGORITHM_LIST, get_varied_parameters, get_default_experiment,\
    NAIVE_SUBSEQ_ALG, INC_SUBSEQ_ALG, MINSEQ_ALG, MAXSEQ_ALG,\
    get_variated_parameters, OPERATOR_LIST, UTIL_ATT_LIST, UTIL_IN, DEF


# Command for experiment run (without parameters for algorithms)
SIMPLE_RUN_COMMAND = "streampref -e {env} -d {det} -m {max}"
# Command for experiment run with temporal preference algorithm option
BESTSEQ_RUN_COMMAND = "streampref -e {env} -d {det} -m {max} -t {alg}"
# Command for experiment run with subsequence algorithm option
SUBSEQ_RUN_COMMAND = "streampref -e {env} -d {det} -m {max} -s {alg}"
# Command for experiment run with statistics output
UTIL_RUN_COMMAND = "streampref -e {env} -o {det} -m {max}"
# Command for calculation of confidence interval
CONFINTERVAL_COMMAND = "confinterval -i {inf} -o {outf} -k {keyf}"

# Dictionary of run commands
RUN_DICT = {}
# CQL run command
RUN_DICT[CQL_ALG] = SIMPLE_RUN_COMMAND
# SEQ run command
RUN_DICT[SEQ_ALG] = SIMPLE_RUN_COMMAND
# Temporal preference run commands
RUN_DICT[BNL_SEARCH_ALG] = SIMPLE_RUN_COMMAND
RUN_DICT[INC_PARTITION_SEQTREE_ALG] = BESTSEQ_RUN_COMMAND
RUN_DICT[INC_PARTITIONLIST_SEQTREE_ALG] = BESTSEQ_RUN_COMMAND
RUN_DICT[INC_PARTITION_SEQTREE_PRUNING_ALG] = BESTSEQ_RUN_COMMAND
RUN_DICT[INC_PARTITIONLIST_SEQTREE_PRUNING_ALG] = BESTSEQ_RUN_COMMAND
# Subsequence run commands
RUN_DICT[NAIVE_SUBSEQ_ALG] = SUBSEQ_RUN_COMMAND
RUN_DICT[INC_SUBSEQ_ALG] = SUBSEQ_RUN_COMMAND
# Filter by length run commands
RUN_DICT[MINSEQ_ALG] = SIMPLE_RUN_COMMAND
RUN_DICT[MAXSEQ_ALG] = SIMPLE_RUN_COMMAND


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
        elif command in [BESTSEQ_RUN_COMMAND, SUBSEQ_RUN_COMMAND]:
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


def run_util(configuration, experiment_conf):
    '''
    Run statistical experiment
    '''
    parameter_conf = configuration[PARAMETER]
    # Get iteration number
    if VAR in parameter_conf[SLI]:
        iterations = experiment_conf[RAN] + max(parameter_conf[SLI][VAR])
    else:
        iterations = experiment_conf[RAN] + parameter_conf[SLI][DEF]
    # Get environment file
    env_file = get_env_util_file(configuration, experiment_conf)
    detail_file = get_detail_util_file(configuration, experiment_conf)
    detail_tmp = detail_file + '.tmp'
    if not os.path.isfile(detail_file):
        command = UTIL_RUN_COMMAND.format(env=env_file, det=detail_tmp,
                                          max=iterations)
        print command
        os.system(command)
        os.rename(detail_tmp, detail_file)
        if not os.path.isfile(detail_file):
            print 'Detail results file not found: ' + detail_file
            print "Check if 'streampref' is in path"


def run_util_experiments(configuration, experiment_list):
    '''
    Run all experiments with statistics output
    '''
    for exp_conf in experiment_list:
        run_util(configuration, exp_conf)


def get_util_summaries(detail_file):
    '''
    Read statistical results from a detail file
    '''
    # Check if file exists
    if not os.path.isfile(detail_file):
        print 'File does not exists: ' + detail_file
        return {att: float('NaN') for att in UTIL_ATT_LIST}
    rec_out = {att: 0.0 for att in UTIL_ATT_LIST}
    in_file = open(detail_file, 'r')
    reader = csv.DictReader(in_file, skipinitialspace=True)
    count = 0
    for rec in reader:
        if rec[UTIL_IN] > 0:
            for att in UTIL_ATT_LIST:
                rec_out[att] += float(rec[att])
        count += 1
    in_file.close()
    for att in UTIL_ATT_LIST:
        rec_out[att] /= count
    return rec_out


def summarize_util(configuration, parameter):
    '''
    Summarize statistical experiments
    '''
    # Result lists
    rec_list = []
    # Get parameter configurations
    par_conf = configuration[PARAMETER]
    # Get default parameter values
    exp_conf = get_default_experiment(par_conf)
    # For every value of current attributes
    for value in par_conf[parameter][VAR]:
        rec = {parameter: value}
        # Get experiment configuration for current value
        exp_conf[parameter] = value
        for op_list in configuration[OPERATOR_LIST]:
            exp_conf[OPERATOR_LIST] = op_list
            dfile = get_detail_util_file(configuration, exp_conf)
            rec_util = get_util_summaries(dfile)
            ope = op_list[-1]
            for att in rec_util:
                rec[ope+att] = rec_util[att]
        rec_list.append(rec)
    # Store summarized results
    filename = get_summary_file(configuration, '', parameter)
    write_result_file(filename, rec_list, parameter)


def summarize_util_operators(configuration):
    '''
    Summarize statistical experiments of operators
    '''
    # Result lists
    rec_list = []
    for op_list in configuration[OPERATOR_LIST]:
        exp_conf = get_default_experiment(configuration[PARAMETER])
        exp_conf[OPERATOR_LIST] = op_list
        dfile = get_detail_util_file(configuration, exp_conf)
        rec_util = get_util_summaries(dfile)
        rec_util['operators'] = str(len(op_list))
        rec_list.append(rec_util)
    # Store summarized results
    filename = get_summary_file(configuration, '', 'operators')
    write_result_file(filename, rec_list, 'operators')


def summarize_all_util(configuration):
    '''
    Summarize all statistical results
    '''
    # Get parameter having variation
    for par in get_variated_parameters(configuration):
        summarize_util(configuration, par)
        summarize_util_operators(configuration)
