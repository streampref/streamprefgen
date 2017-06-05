# -*- coding: utf-8 -*-
'''
Rules for queries with temporal preference operators
'''
from gen.experiment import LEV, RUL, IND, ATT, PARAMETER, MAX_VALUE

# =============================================================================
# Parameters for rules building
# =============================================================================
# Rule components
TYPE = 'type'
PREF = 'pref'
NONPREF = 'nonpref'
COND_SIMPLE = 'simple'
COND_PREV = 'prev'
COND_SOMPREV = 'someprev'
COND_ALLPREV = 'allprev'
INDIFFERENT = 'indiff'
# Types of rules
# Q(A3) and First -> Q+(A2) > Q-(A2) [A4, A5, A6]
FIRST = 'first'
# Q(A3) and Prev Q(A3) and SomePrev Q(A4) and AllPrev Q(A3) ->
# Q+(A2) > Q-(A2) [A4, A5, A6]
PREV = 'prev'
TYPE_LIST = [FIRST, PREV]


def gen_rule(configuration, experiment_conf, rule_type, pref_value):
    '''
    Generate a tcp-rule
    '''
    rule = {}
    rule[TYPE] = rule_type
    # Generate preferred interval
    rule[PREF] = pref_value
    # Generate non preferred interval
    rule[NONPREF] = pref_value+1
    # Indifferent attributes
    rule[INDIFFERENT] = experiment_conf[IND]
    # Q(A3) and Prev(Q(A3)) and SomePrev(Q(A4)) and AllPrev(Q(A5)) ->
    # Q+(A2) > Q-(A2) [A4, A5, A6]
    if rule_type == PREV:
        rule[COND_SIMPLE] = int(configuration[MAX_VALUE] * 0.5)
        rule[COND_PREV] = int(configuration[MAX_VALUE] * 0.5)
        rule[COND_SOMPREV] = int(configuration[MAX_VALUE] * 0.25)
        rule[COND_ALLPREV] = int(configuration[MAX_VALUE] * 0.75)
    # Q(A3) and First -> Q+(A2) > Q-(A2) [A4, A5, A6]
    elif rule_type == FIRST:
        rule[COND_SIMPLE] = int(configuration[MAX_VALUE] * 0.5)
    return rule


def gen_rules_dict(configuration, experiment_conf):
    '''
    Generate a dictionary of rules
    '''
    max_level = experiment_conf[LEV]
    rules_count = experiment_conf[RUL]
    # Dictionary of rules
    rule_dict = {}
    # Start of preference values
    pref_value = 0
    # Current preference level
    level = 0
    # Generate FIRST rules
    rule_list = []
    for _ in range(rules_count/2):
        rule = gen_rule(configuration, experiment_conf, FIRST, pref_value)
        rule_list.append(rule)
        pref_value += 1
        level += 1
        if level == max_level:
            level = 0
            pref_value += 1
    rule_dict[FIRST] = rule_list
    # Generate PREV rules
    level = 0
    pref_value = 0
    rule_list = []
    for _ in range(rules_count/2):
        rule = gen_rule(configuration, experiment_conf, PREV, pref_value)
        rule_list.append(rule)
        pref_value += 1
        level += 1
        if level == max_level:
            level = 0
            pref_value += 1
    rule_dict[PREV] = rule_list
    return rule_dict


def get_rule_string(rule):
    '''
    Get rule string
    '''
    pref = rule[PREF]
    nonpref = rule[NONPREF]
    pref = 'a2 = {p} BETTER a2 = {n}'.format(p=pref, n=nonpref)
    if rule[TYPE] == FIRST:
        cond_str = 'IF (a3 <= {c}) AND FIRST THEN '
        cond_str = cond_str.format(c=rule[COND_SIMPLE])
    elif rule[TYPE] == PREV:
        cond_str = 'IF (a3 <= {c}) AND PREVIOUS (a3 <= {pc}) AND ' + \
            'SOME PREVIOUS (a4 <= {spc}) AND ALL PREVIOUS (a5 <= {apc}) THEN '
        cond_str = cond_str.format(
            c=rule[COND_SIMPLE],
            pc=rule[COND_PREV],
            spc=rule[COND_SOMPREV],
            apc=rule[COND_ALLPREV])
    indiff_count = rule[INDIFFERENT]
    indiff_str = ''
    if indiff_count > 0:
        indiff_list = ['a' + str(att+4) for att in range(indiff_count)]
        indiff_str = '[' + ', '.join(indiff_list) + ']'
    return cond_str + pref + indiff_str


def get_ceteris_attributes(experiment_conf):
    '''
    Get the list of ceteris paribus attributes
    '''
    # Ceteris paribus attributes does no include not include identifier (A1),
    # preference attribute (A2) and indifferent attributes
    return['a'+str(att)
           for att in range(3, experiment_conf[ATT]-experiment_conf[IND]+1)]


def get_temporal_preferences(rules_dict):
    '''
    Convert dictionary of rules into string for preference clause
    '''
    rule_str_list = []
    for rule_type in TYPE_LIST:
        for rule in rules_dict[rule_type]:
            rule_str_list.append(get_rule_string(rule))
    return '\nAND\n'.join(rule_str_list)


def get_rule_list(configuration, experiment_conf):
    '''
    Get rule list from rule dictionary
    '''
    rule_dict = gen_rules_dict(configuration, experiment_conf)
    rule_list = []
    for rule_type in TYPE_LIST:
        rule_list += rule_dict[rule_type]
    return rule_list
