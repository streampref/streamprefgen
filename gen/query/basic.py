# -*- coding: utf-8 -*-
'''
Basic for queries modules
'''

from gen.directory import get_data_file, get_tup_file
from gen.experiment import ATT, INTEGER, get_attribute_list


# =============================================================================
# Strings for registration in environment file
# =============================================================================
REG_STREAM_STR = "REGISTER STREAM s ({atts}) \nINPUT '{dfile}';"
REG_TUP_STR = "REGISTER TABLE tup (A2 INTEGER, A3 INTEGER) \nINPUT '{dfile}';"
REG_Q_STR = "\n\nREGISTER QUERY {qname} \nINPUT '{qfile}';"
REG_Q_OUTPUT_STR = \
    "\n\nREGISTER QUERY {qname} \nINPUT '{qfile}' \nOUTPUT '{ofile}';"


def get_register_stream(configuration, experiment_conf, include_tup=False):
    '''
    Get register steam string
    '''
    # Get attribute list
    att_list = get_attribute_list(experiment_conf[ATT])
    att_list = [att + ' ' + INTEGER for att in att_list]
    att_str = ', '.join(att_list)
    # Get data filename
    filename = get_data_file(configuration, experiment_conf)
    # Register stream
    text = REG_STREAM_STR.format(atts=att_str, dfile=filename)
    if include_tup:
        text += '\n\n'
        # Register tup table
        filename = get_tup_file(configuration)
        text += REG_TUP_STR.format(dfile=filename)
    text += '\n\n' + '#' * 80 + '\n\n'
    return text
