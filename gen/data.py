# -*- coding: utf-8 -*-
'''
Data stream generation
'''

import random

from gen.directory import write_to_csv, get_data_file, append_to_csv
from gen.experiment import ATT, NSQ, TS_ATT, PARAMETER, RAN, PCT, \
    get_attribute_list, get_max_data_timestamp, TUPLE_RATE, MAX_VALUE


def gen_sequence_id_list(sequence_number):
    '''
    Generate the list of all possible identifier for sequences
    '''
    # Create the list of identifier records
    return [{'A1': value} for value in range(sequence_number)]


def gen_records(configuration, experiment_conf, id_list, timestamp):
    '''
    Generate records for a timestamp
    '''
    sequences_per_instant = int(experiment_conf[NSQ] *
                                configuration[TUPLE_RATE])
    # Randomize list of identifiers
    random.shuffle(id_list)
    rec_list = []
    # Loop to count identifier
    for _ in range(sequences_per_instant):
        # Get an identifier
        rec = id_list.pop(0)
        # Put it back into list
        id_list.append(rec)
        # Create new record
        new_rec = rec.copy()
        # Generate random values for attributes (excluding identifier)
        for att in range(2, experiment_conf[ATT] + 1):
            new_rec['A' + str(att)] = \
                random.randint(0, configuration[MAX_VALUE] - 1)
        # Include timestamp attribute
        new_rec[TS_ATT] = timestamp
        # Append to list
        rec_list.append(new_rec)
    return rec_list


def generate(timestamp, conseq_percent, start):
    '''
    Return if a timestamp must be generated according to
    percentage consecutive
    '''
    if conseq_percent == 0.0 and (timestamp+start) % 2 == 0:
        return False
    elif conseq_percent == 0.25 and ((timestamp+start) % 4 == 0 or
                                     ((timestamp+start) - 1) % 4 == 0):
        return False
    elif conseq_percent == 0.5 and (timestamp+start) % 4 == 0:
        return False
    elif conseq_percent == 0.75 and (timestamp+start) % 8 == 0:
        return False
    else:
        return True


def gen_conseq_records(configuration, experiment_conf,
                       start_id_list, timestamp):
    '''
    Generate records for a timestamp
    '''
    rec_list = []
    # Loop to count identifier
    for id_rec, start in start_id_list:
        if generate(timestamp, experiment_conf[PCT], start):
            # Create new record
            new_rec = id_rec.copy()
            # Generate random values for attributes (excluding identifier)
            for att in range(2, experiment_conf[ATT] + 1):
                new_rec['A' + str(att)] = \
                    random.randint(0, configuration[MAX_VALUE] - 1)
            # Include timestamp attribute
            new_rec[TS_ATT] = timestamp
            # Append to list
            rec_list.append(new_rec)
    return rec_list


def gen_stream(configuration, experiment_conf):
    '''
    Generate a data stream
    '''
    # Build attribute list
    att_list = get_attribute_list(experiment_conf[ATT], include_timestamp=True)
    # Get list of sequence identifiers
    id_list = gen_sequence_id_list(experiment_conf[NSQ])
    # Get maximum timestamp (maximum range + maximum slide)
    max_ts = get_max_data_timestamp(configuration[PARAMETER])
    filename = get_data_file(configuration, experiment_conf)
    # First timestamp
    rec_list = gen_records(configuration, experiment_conf,
                           id_list, 0)
    write_to_csv(filename, att_list, rec_list)
    # For each timestamp
    for timestamp in range(1, max_ts + 1):
        rec_list = gen_records(configuration, experiment_conf,
                               id_list, timestamp)
        append_to_csv(filename, att_list, rec_list)


def gen_conseq_stream(configuration, experiment_conf):
    '''
    Generate data stream
    '''
    # Build attribute list
    att_list = get_attribute_list(experiment_conf[ATT], include_timestamp=True)
    # Get list of sequence identifiers
    id_list = gen_sequence_id_list(experiment_conf[NSQ])
    # Randomize start timestamp for every identifier
    id_start_list = []
    for rec in id_list:
        start = random.randint(0, experiment_conf[RAN]-1)
        id_start_list.append((rec, start))
    # Get maximum timestamp (maximum range + maximum slide)
    max_ts = get_max_data_timestamp(configuration[PARAMETER])
    # File
    filename = get_data_file(configuration, experiment_conf)
    # First instant
    rec_list = gen_conseq_records(configuration, experiment_conf,
                                  id_start_list, 0)
    write_to_csv(filename, att_list, rec_list)
    # For each timestamp
    for timestamp in range(1, max_ts+1):
        rec_list = gen_conseq_records(configuration, experiment_conf,
                                      id_start_list, timestamp)
        append_to_csv(filename, att_list, rec_list)


def gen_all_streams(configuration, experiment_list):
    '''
    Generate all streams
    '''
    for exp_conf in experiment_list:
        gen_stream(configuration, exp_conf)


def gen_all_conseq_streams(configuration, experiment_list):
    '''
    Generate all streams
    '''
    for exp_conf in experiment_list:
        gen_conseq_stream(configuration, exp_conf)
