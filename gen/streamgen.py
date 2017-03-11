# -*- coding: utf-8 -*-
'''
Data stream generation
'''

import os
import random

from gen.directory import DATA_DIR, write_to_csv
from gen.experiment import ATT, NSQ, TS_ATT, MAX_VALUE, IDA, \
    PSI, SLI, RAN, get_max_value, get_attribute_list, get_data_id


def gen_sequence_id_list(id_attribute_number, sequence_number):
    '''
    Generate the list of all possible identifier for sequences
    '''
    # List of identifier attributes
    id_att_list = get_attribute_list(id_attribute_number)
    # Get first identifier attribute
    id_att = id_att_list.pop(0)
    # Create the list of identifier records
    # (initially with first identifier attribute)
    id_rec_list = [{id_att: value} for value in range(MAX_VALUE)]
    # Process remaining attributes
    while len(id_att_list):
        # Create a new list
        new_id_rec_list = []
        # Get next attribute
        id_att = id_att_list.pop(0)
        # For every attribute value
        for value in range(MAX_VALUE):
            # For every existing record
            for rec in id_rec_list:
                # Copy record
                new_rec = rec.copy()
                # Add current value for current attribute
                new_rec[id_att] = value
        # Update list of record
        id_rec_list = new_id_rec_list
    # Return just the identifier enough to sequence number
    return id_rec_list[:sequence_number]


def gen_records(attributes_number, id_attributes,
                sequences_per_instant, id_list, timestamp):
    '''
    Generate records for a timestamp
    '''
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
        # Generate random values for attributes (excluding identifiers)
        for att in range(id_attributes + 1, attributes_number + 1):
            new_rec['A' + str(att)] = random.randint(0, MAX_VALUE - 1)
        # Include timestamp attribute
        new_rec[TS_ATT] = timestamp
        # Append to list
        rec_list.append(new_rec)
    return rec_list


def gen_stream(experiment_conf, parameter_conf, directory_dict):
    '''
    Generate a data stream
    '''
    # Build attribute list
    att_list = get_attribute_list(experiment_conf[ATT])
    # Get list of sequence identifiers
    id_list = gen_sequence_id_list(experiment_conf[IDA], experiment_conf[NSQ])
    # List of records to be returned
    rec_list = []
    # Number of sequence identifiers per instant
    seq_per_instant = int(experiment_conf[PSI] * experiment_conf[NSQ])
    # Get iterations number (maximum range + maximum slide)
    ite = get_max_value(parameter_conf, RAN) + \
        get_max_value(parameter_conf, SLI)
    # For each timestamp
    for timestamp in range(ite):
        rec_list += gen_records(experiment_conf[ATT], experiment_conf[IDA],
                                seq_per_instant, id_list, timestamp)
    # Insert timstamp into attribute list
    att_list.insert(0, TS_ATT)
    # Open output file
    filename = directory_dict[DATA_DIR] + os.sep + 's' \
        + get_data_id(experiment_conf, parameter_conf) + '.csv'
    write_to_csv(filename, att_list, rec_list)


def gen_all_streams(experiment_list, directory_dict, parameter_conf):
    '''
    Generate all streams
    '''
    for exp_conf in experiment_list:
        gen_stream(exp_conf, directory_dict, parameter_conf)
