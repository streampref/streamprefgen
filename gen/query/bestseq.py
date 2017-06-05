# -*- coding: utf-8 -*-
'''
Queries for experiments with temporal preference operators
'''

import os

from gen.directory import write_to_txt, write_to_csv, get_env_file, \
    get_tup_file, get_query_dir, get_out_file
from gen.experiment import ALGORITHM, CQL_ALG, TS_ATT, FL_ATT, \
    RAN, SLI, ATT, LEV, get_attribute_list, MAX_VALUE
from gen.query.basic import REG_Q_STR, \
    REG_Q_OUTPUT_STR, get_register_stream
from gen.query.rule import gen_rules_dict, get_rule_list, \
    get_temporal_preferences, TYPE, FIRST, PREV, COND_PREV, COND_SOMPREV, \
    COND_ALLPREV, COND_SIMPLE, PREF, NONPREF, get_ceteris_attributes


# =============================================================================
# StreamPref Query
# =============================================================================
BESTSEQ_QUERY = '''
SELECT SEQUENCE IDENTIFIED BY a1 [RANGE {ran} SECOND, SLIDE {sli} SECOND]
FROM s TEMPORAL PREFERENCES
{pref};
'''

# =============================================================================
# CQL Queries
# =============================================================================
# Query for sequence extraction
Z_QUERY = '''
SELECT SEQUENCE IDENTIFIED BY a1 [RANGE {ran} SECOND, SLIDE {sli} SECOND]
FROM s;
'''

# Join same positions
P_JOIN_QUERY = '''
SELECT z1._pos, z1.a1 AS x1, {z1_att}, z2.a1 AS x2, {z2_att}
FROM z AS z1, z AS z2 WHERE z1._pos =  z2._pos;
'''

# Smaller non correspondent position (positions to be compared)
P_QUERY = '''
SELECT MIN(_pos) AS _pos, x1, x2 FROM p_join WHERE {p_filter}
GROUP BY x1, x2;
'''

# Query equivalent to rules with FIRST condition
FIRST_QUERY = '''
SELECT _pos, x1 FROM p WHERE _pos = 1;
'''

# Query equivalent to rules with PREVIOUS conditions
PREV_QUERY = '''
SELECT p._pos, p.x1 FROM z, p
WHERE p.x1 = z.a1 AND p._pos = z._pos+1 AND z.a3 >= {cond};
'''

# Minimum positions where the rule condition is valid
MIN_SOMEPREV_QUERY = '''
SELECT MIN(_pos) AS _pos, a1 AS x1 FROM z WHERE z.a4 >= {cond} GROUP BY a1;
'''

# Query equivalent to rules with SOME PREVIOUS conditions
SOMEPREV_QUERY = '''
SELECT p._pos, p.x1 FROM p, m_sp{rn} AS msp
WHERE p.x1 = msp.x1 AND p._pos > msp._pos;
'''

# Queries equivalent to rules with ALL PREVIOUS conditions
# Positions where rule condition is not valid
# Include maximum position (maybe rule condition is valid at all positions)
NONVALID_ALLPREV_QUERY = '''
SELECT MAX(_pos) AS _pos, x1 FROM p GROUP BY x1
UNION
SELECT a1 AS x1, _pos FROM z WHERE NOT a5 >= {cond};
'''
# Minimum position among those not satisfying the rule condition
MIN_ALLPREV_QUERY = '''
SELECT MIN(_pos) AS _pos, x1 FROM nv_ap{rn} GROUP BY x1;
'''
ALLPREV_QUERY = '''
SELECT p._pos, p.x1 FROM p, m_ap{rn} AS pmin
WHERE p.x1 = pmin.x1 AND p._pos <= pmin._pos AND p._pos > 1;
'''

# Query to join relation formulas of a condition
COND_QUERY = '''
SELECT * FROM r{rn}_f1 AS f1, r{rn}_f2 AS f2, r{rn}_f3 AS f3
WHERE f1._pos = f2._pos AND f1.x1 = f2.x1
  AND f2._pos = f3._pos AND f2.x1 = f3.x1;
'''

# Direct comparisons
# Preferred tuples according to rule i
DI_PREF_QUERY = '''
SELECT r._pos, r.x1, {att}, 1 AS t FROM r{rn} AS r, z
WHERE r._pos = z._pos AND r.x1 = z.a1 AND a3 <= {cond} AND a2 = {pref}
UNION
SELECT r._pos, r.x1, t.a2, t.a3, {attnt}, 0 AS t
FROM r{rn} AS r, z, tup AS t
WHERE r._pos = z._pos AND r.x1 = z.a1 AND t.a3 <= {cond} AND t.a2 = {pref};
'''
# Non preferred tuples according to rule i
DI_NONPREF_QUERY = '''
SELECT r._pos, r.x1 AS x2, {att}, 1 AS t FROM r{rn} AS r, z
WHERE r._pos = z._pos AND r.x1 = z.a1 AND a3 <= {cond} AND a2 = {nonpref}
UNION
SELECT r._pos, r.x1 AS x2, t.a2, t.a3, {attnt}, 0 AS t
FROM r{rn} AS r, z, tup AS t
WHERE r._pos = z._pos AND r.x1 = z.a1 AND t.a3 <= {cond} AND t.a2 = {nonpref};
'''

# Direct comparisons (containing only present condition)
DI_QUERY = '''
SELECT ri._pos, ri.x1, ri.x2, {p_att}, p.t, {np_att}, np.t AS _t
FROM p AS ri, d{rn}_pref AS p, d{rn}_nonpref AS np
WHERE ri._pos = p._pos AND ri._pos = np._pos
AND ri.x1 = p.x1 AND ri.x2 = np.x2
AND {cet_cond};
'''

# Transitive comparisons
TI_QUERY = '''
SELECT p._pos, p.x1, np.x2, {p_att}, p.t, {np_att}, np._t
FROM t{prev_n} AS p, t{prev_n} AS np
WHERE p._pos = np._pos AND p.x1 = np.x1 AND p.x2 = np.x2
AND {p_np_join}
UNION
SELECT * FROM t{prev_n};
'''

# Identifiers of dominant sequences
ID_QUERY = '''
SELECT DISTINCT a1 FROM z
EXCEPT
SELECT DISTINCT x2 AS a1 FROM t{rn} WHERE t = 1 AND _t = 1;
'''

# Dominant sequences
BEST_QUERY = '''
SELECT z.* FROM z, id
WHERE z.a1 = id.a1;
'''


def gen_transitive_tup(configuration, tup_file):
    '''
    Generate tuples for equivalence of transitive closure
    '''
    rec_list = []
    # Only A2 and A3 have present predicates
    for a2_value in range(configuration[MAX_VALUE]):
        rec = {TS_ATT: 0, FL_ATT: '+', 'a2': a2_value}
        for a3_value in range(configuration[MAX_VALUE]):
            rec = rec.copy()
            rec['a3'] = a3_value
            rec_list.append(rec)
    att_name_list = [TS_ATT, FL_ATT, 'a2', 'a3']
    # Store records on file
    write_to_csv(tup_file, att_name_list, rec_list)


def gen_bestseq_query(configuration, experiment_conf):
    '''
    Generate StreamPref queries with BESTSEQ operator
    '''
    filename = get_query_dir(configuration, experiment_conf) + \
        os.sep + 'bestseq.cql'
    rules_dict = gen_rules_dict(configuration, experiment_conf)
    pref_str = get_temporal_preferences(rules_dict)
    query = BESTSEQ_QUERY.format(ran=experiment_conf[RAN],
                                 sli=experiment_conf[SLI],
                                 pref=pref_str)
    write_to_txt(filename, query)


def gen_first_rule_query(out_dir, rule_number):
    '''
    Generate query for rule with first condition
    '''
    filename = out_dir + os.sep + 'r' + str(rule_number) + '.cql'
    write_to_txt(filename, FIRST_QUERY)


def gen_prev_rule_query(out_dir, rule_number, rule, condition_number):
    '''
    Generate query for rule with previous condition
    '''
    filename = out_dir + os.sep + \
        'r' + str(rule_number) + '_f' + str(condition_number) + '.cql'
    query = PREV_QUERY.format(cond=rule[COND_PREV])
    write_to_txt(filename, query)


def gen_someprev_rule_query(out_dir, rule_number, rule, condition_number):
    '''
    Generate query for rule with some previous condition
    '''
    filename = out_dir + os.sep + 'm_sp' + str(rule_number) + '.cql'
    query = MIN_SOMEPREV_QUERY.format(cond=rule[COND_SOMPREV])
    write_to_txt(filename, query)
    filename = out_dir + os.sep + \
        'r' + str(rule_number) + '_f' + str(condition_number) + '.cql'
    query = SOMEPREV_QUERY.format(rn=rule_number)
    write_to_txt(filename, query)


def gen_allprev_rule_query(out_dir, rule_number, rule, condition_number):
    '''
    Generate query for rule with all previous condition
    '''
    filename = out_dir + os.sep + 'nv_ap' + str(rule_number) + '.cql'
    query = NONVALID_ALLPREV_QUERY.format(cond=rule[COND_ALLPREV])
    write_to_txt(filename, query)
    filename = out_dir + os.sep + 'm_ap' + str(rule_number) + '.cql'
    query = MIN_ALLPREV_QUERY.format(rn=rule_number)
    write_to_txt(filename, query)
    filename = out_dir + os.sep + \
        'r' + str(rule_number) + '_f' + str(condition_number) + '.cql'
    query = ALLPREV_QUERY.format(rn=rule_number)
    write_to_txt(filename, query)


def gen_rule_queries(query_dir, experiment_conf, rule_number, rule):
    '''
    Generate queries for all rules
    '''
    if rule[TYPE] == FIRST:
        gen_first_rule_query(query_dir, rule_number)
    elif rule[TYPE] == PREV:
        gen_prev_rule_query(query_dir, rule_number, rule, 1)
        gen_someprev_rule_query(query_dir, rule_number, rule, 2)
        gen_allprev_rule_query(query_dir, rule_number, rule, 3)
        query = COND_QUERY.format(rn=rule_number)
        filename = query_dir + os.sep + 'r' + str(rule_number) + '.cql'
        write_to_txt(filename, query)
    # All input attributes (except identifier)
    # Get attribute list
    att_list = get_attribute_list(experiment_conf[ATT])
    # Exclude sequence identifier (A1)
    att_list = att_list[1:]
    att_list = ', '.join(att_list)
    # Attributes non in TUP (transitive tuples)
    attnt_list = get_attribute_list(experiment_conf[ATT])
    # Exclude sequence identifier (A1) and TUP attributes (A2, A3)
    attnt_list = attnt_list[3:]
    attnt_list = ', '.join(attnt_list)
    # Generate D_i Pref Queries
    query = DI_PREF_QUERY.format(att=att_list, attnt=attnt_list,
                                 cond=rule[COND_SIMPLE],
                                 pref=rule[PREF], rn=rule_number)
    filename = query_dir + os.sep + 'd' + str(rule_number) + '_pref.cql'
    write_to_txt(filename, query)
    # Generate D_i NonPref Queries
    query = \
        DI_NONPREF_QUERY.format(att=att_list, attnt=attnt_list,
                                cond=rule[COND_SIMPLE],
                                nonpref=rule[NONPREF], rn=rule_number)
    filename = query_dir + os.sep + 'd' + str(rule_number) + '_nonpref.cql'
    write_to_txt(filename, query)
    # Generate D_i Queries
    # Get attribute list
    att_list = get_attribute_list(experiment_conf[ATT])
    # Exclude sequence identifier (A1)
    att_list = att_list[1:]
    p_att_list = ['p.' + att for att in att_list]
    p_att_list = ', '.join(p_att_list)
    np_att_list = ['p.' + att + ' AS _' + att for att in att_list]
    np_att_list = ', '.join(np_att_list)
    ceteris_cond = get_ceteris_attributes(experiment_conf)
    ceteris_cond = ['p.' + att + ' = np.' + att for att in ceteris_cond]
    ceteris_cond = ' AND '.join(ceteris_cond)
    filename = query_dir + os.sep + 'd' + str(rule_number) + '.cql'
    query = DI_QUERY.format(p_att=p_att_list, np_att=np_att_list,
                            rn=rule_number, cet_cond=ceteris_cond)
    write_to_txt(filename, query)


def gen_cql_transitive_queries(experiment_conf, query_dir):
    '''
    Generate CQL queries for transitive comparisons
    '''
    # Generate T_i Queries
    # Get attribute list
    att_list = get_attribute_list(experiment_conf[ATT])
    # Exclude sequence identifier (A1)
    att_list = att_list[1:]
    p_att_list = ['p.' + att for att in att_list]
    p_att_list = ', '.join(p_att_list)
    np_att_list = ['np._' + att for att in att_list]
    np_att_list = ', '.join(np_att_list)
    join_att = ['p._' + att + ' = np.' + att for att in att_list]
    join_att = ' AND '.join(join_att)
    for level_number in range(2, experiment_conf[LEV] + 1):
        filename = query_dir + os.sep + 't' + str(level_number) + '.cql'
        prev_level = level_number - 1
        query = TI_QUERY.format(prev_n=prev_level, p_att=p_att_list,
                                np_att=np_att_list, p_np_join=join_att)
        write_to_txt(filename, query)


def gen_cql_queries(configuration, experiment_conf):
    '''
    Generate queries with CQL original operators equivalent to BESTSEQ operator
    '''
    filename = get_tup_file(configuration)
    gen_transitive_tup(configuration, filename)
    query_dir = get_query_dir(configuration, experiment_conf)
    # Generate z query (sequences)
    query = Z_QUERY.format(ran=experiment_conf[RAN],
                           sli=experiment_conf[SLI])
    filename = query_dir + os.sep + 'z.cql'
    write_to_txt(filename, query)
    # Generate p_join query (join z positions)
    # Get attribute list
    att_list = get_attribute_list(experiment_conf[ATT])
    # Exclude sequence identifier (A1)
    att_list = att_list[1:]
    z1_att_list = ['z1.' + att for att in att_list]
    z1_att_list = ', '.join(z1_att_list)
    z2_att_list = ['z2.' + att + ' AS _' + att for att in att_list]
    z2_att_list = ', '.join(z2_att_list)
    query = P_JOIN_QUERY.format(z1_att=z1_att_list, z2_att=z2_att_list)
    filename = query_dir + os.sep + 'p_join.cql'
    write_to_txt(filename, query)
    # Generate query p (positions to be compared)
    diff_filter = ['NOT ' + att + ' = _' + att for att in att_list]
    diff_filter = ' OR '.join(diff_filter)
    query = P_QUERY.format(p_filter=diff_filter)
    filename = query_dir + os.sep + 'p.cql'
    write_to_txt(filename, query)
    # Get rule list
    rule_list = get_rule_list(configuration, experiment_conf)
    # Generate query t1 (identifier of dominant sequences) and
    # individual rule queries
    query_list = []
    for index, rule in enumerate(rule_list):
        # Generates queries R_i and D_i for each rule
        gen_rule_queries(query_dir, experiment_conf, index + 1, rule)
        query = 'SELECT * FROM d' + str(index + 1)
        query_list.append(query)
    query = '\nUNION\n'.join(query_list) + ';'
    filename = query_dir + os.sep + 't1.cql'
    write_to_txt(filename, query)
    # Generate T_i Queries
    gen_cql_transitive_queries(experiment_conf, query_dir)
    # Generate ID query
    query = ID_QUERY.format(rn=experiment_conf[LEV])
    filename = query_dir + os.sep + 'id.cql'
    write_to_txt(filename, query)
    # Generate query for final result
    query = 'SELECT z.* FROM z, id WHERE z.a1 = id.a1;'
    filename = query_dir + os.sep + 'equiv.cql'
    write_to_txt(filename, query)


def gen_all_queries(configuration, experiment_list):
    '''
    Generate all queries
    '''
    for exp_conf in experiment_list:
        if exp_conf[ALGORITHM] == CQL_ALG:
            gen_cql_queries(configuration, exp_conf)
        else:
            gen_bestseq_query(configuration, exp_conf)


def gen_register_di(query_dir, rule_count):
    '''
    Generate register text for Di queries
    '''
    query_name = 'd' + str(rule_count) + '_pref'
    filename = query_dir + os.sep + query_name + '.cql'
    text = REG_Q_STR.format(qname=query_name, qfile=filename)
    query_name = 'd' + str(rule_count) + '_nonpref'
    filename = query_dir + os.sep + query_name + '.cql'
    text += REG_Q_STR.format(qname=query_name, qfile=filename)
    query_name = 'd' + str(rule_count)
    filename = query_dir + os.sep + query_name + '.cql'
    text += REG_Q_STR.format(qname=query_name, qfile=filename)
    return text


def gen_reg_rules_queries(configuration, query_dir, experiment_conf):
    '''
    Generate rules queries
    '''
    text = ''
    # Get rule list
    rule_list = get_rule_list(configuration, experiment_conf)
    for r_count, rule in enumerate(rule_list):
        if rule[TYPE] == FIRST:
            # ri (first)
            query_name = 'r' + str(r_count + 1)
            filename = query_dir + os.sep + query_name + '.cql'
            text += REG_Q_STR.format(qname=query_name, qfile=filename)
        # Simple rules
        elif rule[TYPE] == PREV:
            # prev formula
            query_name = 'r' + str(r_count + 1) + '_f1'
            filename = query_dir + os.sep + query_name + '.cql'
            text += REG_Q_STR.format(qname=query_name, qfile=filename)
            # someprev formula
            # m_sp (min_someprev)
            query_name = 'm_sp' + str(r_count + 1)
            filename = query_dir + os.sep + query_name + '.cql'
            text += REG_Q_STR.format(qname=query_name, qfile=filename)
            # ri (someprev)
            query_name = 'r' + str(r_count + 1) + '_f2'
            filename = query_dir + os.sep + query_name + '.cql'
            text += REG_Q_STR.format(qname=query_name, qfile=filename)
            # allprev formula
            # nv_ap (nonvalid_allprev)
            query_name = 'nv_ap' + str(r_count + 1)
            filename = query_dir + os.sep + query_name + '.cql'
            text += REG_Q_STR.format(qname=query_name, qfile=filename)
            # m_ap (min_allprev)
            query_name = 'm_ap' + str(r_count + 1)
            filename = query_dir + os.sep + query_name + '.cql'
            text += REG_Q_STR.format(qname=query_name, qfile=filename)
            # ri (allprev)
            query_name = 'r' + str(r_count + 1) + '_f3'
            filename = query_dir + os.sep + query_name + '.cql'
            text += REG_Q_STR.format(qname=query_name, qfile=filename)
            # ri
            query_name = 'r' + str(r_count + 1)
            filename = query_dir + os.sep + query_name + '.cql'
            text += REG_Q_STR.format(qname=query_name, qfile=filename)
        text += gen_register_di(query_dir, r_count + 1)
    return text


def gen_cql_env(configuration, experiment_conf, output):
    '''
    Generate environment files for StremPref
    '''
    # Register stream
    text = get_register_stream(configuration, experiment_conf,
                               include_tup=True)
    query_dir = get_query_dir(configuration, experiment_conf)
    filename = query_dir + os.sep + 'z.cql'
    text += REG_Q_STR.format(qname='z', qfile=filename)
    filename = query_dir + os.sep + 'p_join.cql'
    text += REG_Q_STR.format(qname='p_join', qfile=filename)
    filename = query_dir + os.sep + 'p.cql'
    text += REG_Q_STR.format(qname='p', qfile=filename)
    text += gen_reg_rules_queries(configuration, query_dir, experiment_conf)
    query_name = 't1'
    filename = query_dir + os.sep + query_name + '.cql'
    text += REG_Q_STR.format(qname=query_name, qfile=filename)
    level = experiment_conf[LEV]
    for number in range(2, level + 1):
        query_name = 't' + str(number)
        filename = query_dir + os.sep + query_name + '.cql'
        text += REG_Q_STR.format(qname=query_name, qfile=filename)
    query_name = 'id'
    filename = query_dir + os.sep + query_name + '.cql'
    text += REG_Q_STR.format(qname=query_name, qfile=filename)
    query_name = 'equiv'
    filename = query_dir + os.sep + query_name + '.cql'
    if output:
        # Get output filename
        out_file = get_out_file(configuration, experiment_conf)
        text += REG_Q_OUTPUT_STR.format(qname=query_name, qfile=filename,
                                        ofile=out_file)
    else:
        text += REG_Q_STR.format(qname=query_name, qfile=filename)
    # Get environment filename
    filename = get_env_file(configuration, experiment_conf)
    write_to_txt(filename, text)


def gen_bestseq_env(configuration, experiment_conf, output):
    '''
    Generate environment files StreamPref operator
    '''
    # Register stream
    text = get_register_stream(configuration, experiment_conf)
    # Get query filename
    filename = get_query_dir(configuration, experiment_conf) + \
        os.sep + 'bestseq.cql'
    # Register query
    if output:
        # Get output filename
        out_file = get_out_file(configuration, experiment_conf)
        text += REG_Q_OUTPUT_STR.format(qname='bestseq', qfile=filename,
                                        ofile=out_file)
    else:
        text += REG_Q_STR.format(qname='bestseq', qfile=filename)
    # Get environment filename
    filename = get_env_file(configuration, experiment_conf)
    write_to_txt(filename, text)


def gen_all_env(configuration, experiment_list, output=False):
    '''
    Generate all environment files
    '''
    for exp_conf in experiment_list:
        if exp_conf[ALGORITHM] == CQL_ALG:
            gen_cql_env(configuration, exp_conf, output)
        else:
            gen_bestseq_env(configuration, exp_conf, output)
