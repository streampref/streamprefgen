# -*- coding: utf-8 -*-
'''
Experiments execution
'''

import csv
import os

from gen.directory import get_detail_file, get_env_file, \
    SUMMARY_DIR, write_result_file, RESULT_DIR
from gen.experiment import ALG, RAN, VAR, SLI, TPREF_RUN_OPTION, \
    DEF, RUNTIME, MEMORY, get_variated_parameters, CQL_ALG


# Command for experiment run
RUN_COMMAND = "streampref -e {env} -d {det} -m {max}"
# Command for experiment run with temporal preference algorithm option
TPREF_RUN_COMMAND = "streampref -e {env} -d {det} -m {max} -t {alg}"
# Command for calculation of confidence interval
CONFINTERVAL_COMMAND = "confinterval -i {inf} -o {outf} -k {keyf}"


def run(experiment_conf, parameter_conf, directory_dict, count,
        run_option):
    '''
    Run an experiment
    '''
    # Get iteration number
    iterations = experiment_conf[RAN] + max(parameter_conf[SLI][VAR])
    # Get environment file
    env_file = get_env_file(experiment_conf, parameter_conf, directory_dict)
    # Get detail file
    detail_file = get_detail_file(experiment_conf, parameter_conf,
                                  directory_dict, count)
    if not os.path.isfile(detail_file):
        if run_option is None or experiment_conf[ALG] == CQL_ALG:
            command = RUN_COMMAND.format(env=env_file, det=detail_file,
                                         max=iterations)
        elif run_option == TPREF_RUN_OPTION:
            command = TPREF_RUN_COMMAND.format(env=env_file, det=detail_file,
                                               max=iterations,
                                               alg=experiment_conf[ALG])
        print command
        os.system(command)
        if not os.path.isfile(detail_file):
            print 'Detail results file not found: ' + detail_file
            print "Check if 'streampref' is in path"


def run_experiments(experiment_list, parameter_conf, directory_dict,
                    run_count, run_option=None):
    '''
    Run all experiments
    '''
    for count in range(1, run_count + 1):
        for exp_conf in experiment_list:
            run(exp_conf, parameter_conf, directory_dict, count,
                run_option)


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


def summarize(parameter, parameter_conf,  # IGNORE:too-many-locals
              directory_dict, algorithm_list,  # IGNORE:too-many-locals
              run_count):  # IGNORE:too-many-locals
    '''
    Summarize experiments about range variation
    '''
    # Get parameter having variation
    par_list = get_variated_parameters(parameter_conf)
    # Result lists
    time_list = []
    mem_list = []
    exp_conf = {par: parameter_conf[par][DEF] for par in par_list}
    # For every value of current attributes
    for value in parameter_conf[parameter][VAR]:
        # Get experiment configuration for current value
        exp_conf[parameter] = value
        # For every execution
        for count in range(1, run_count + 1):
            # Creates record for current parameter and value
            time_rec = {parameter: value}
            mem_rec = {parameter: value}
            # For every algorithm
            for alg in algorithm_list:
                exp_conf[ALG] = alg
                # Get detail file
                filename = get_detail_file(exp_conf, parameter_conf,
                                           directory_dict, count)
                # Get summarized results
                runtime, memory = get_summaries(filename)
                time_rec[alg] = runtime
                mem_rec[alg] = memory
            # Append to result lists
            time_list.append(time_rec)
            mem_list.append(mem_rec)
    # Store summarized results
    filename = directory_dict[SUMMARY_DIR] + os.sep + \
        'run_' + parameter + '.csv'
    write_result_file(filename, time_list, parameter)
    filename = directory_dict[SUMMARY_DIR] + os.sep + \
        'mem_' + parameter + '.csv'
    write_result_file(filename, mem_list, parameter)


def summarize_all(parameter_conf, directory_dict, algorithm_list, run_count):
    '''
    Summarize all results
    '''
    # Get parameter having variation
    par_list = get_variated_parameters(parameter_conf)
    for par in par_list:
        summarize(par, parameter_conf, directory_dict, algorithm_list,
                  run_count)


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


def confidence_interval_all(parameter_conf, directory_dict):
    '''
    Calculate confidence interval for all summarized results
    '''
    par_list = get_variated_parameters(parameter_conf)
    # For every parameter
    for parameter in par_list:
        in_file = directory_dict[SUMMARY_DIR] + os.sep + \
            'run_' + parameter + '.csv'
        out_file = directory_dict[RESULT_DIR] + os.sep + \
            'run_' + parameter + '.csv'
        confidence_interval(parameter, in_file, out_file)
        in_file = directory_dict[SUMMARY_DIR] + os.sep + \
            'mem_' + parameter + '.csv'
        out_file = directory_dict[RESULT_DIR] + os.sep + \
            'mem_' + parameter + '.csv'
        confidence_interval(parameter, in_file, out_file)
