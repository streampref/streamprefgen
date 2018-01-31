# Table of Contents

- [Introduction](#introduction)
- [Generators](#generators)
- [Algorithms](#algorithms)
- [Parameters](#parameters)
- [Command Line](#command-line)

# Introduction

StreamPrefGen is a dataset generator for evaluation of the operators of the StreamPref query language.
The operators are implemented in the StreamPref Data Stream Management System (DSMS) prototype.
The StreamPrefGen is composed of tools to generate streams, relations, queries and auxiliary files for the execution of experiments with StreamPref.

# Generators

The StreamPrefGen is composed by individual dataset generators for evaluation of specific StreamPref operators.
The generators are the following:
- __bestseqgen.py__: generator for evaluation of __BESTSEQ__ operator (temporal preference operator);
- __seqgen.py__: generator for evaluation of __SEQ__ operator (sequence extraction);
- __conseggen.py__: generator for evaluation of __CONSEQ__ operator (subsequences with consecutive tuples);
- __endseqgen.py__: generator for evaluation of __ENDSEQ__ operator (subsequences with the last position);
- __maxseqgen.py__: generator for evaluation of __MAXSEQ__ operator (Filtering by maximum length);
- __minseqgen.py__: generator for evaluation of __MINSEQ__ operator (Filtering by minimum length);
- __utilgen.py__: generator for utility experiments.

# Algorithms

Except by the __utilgen.py__, all tools generate StremPref environments for evaluating their operators.
Each operator can be evaluated by one or more algorithms and by a CQL equivalent query.
The available algorithms for each operator are the following:
- __SEQ__
	- Incremental algorithm
	- CQL Equivalence
- __CONSEQ__ / __ENDSEQ__
	- Naive algorithm
	- Incremental algorithm
	- CQL Equivalence
- __MINSEQ__ / __MAXSEQ__
	- Direct algorithm
	- CQL Equivalence
- __BESTSEQ__
	- Naive algorithm with depth search comparison
	- Incremental algorithm with sequences tree
	- Incremental algorithm with sequences tree and pruning
	- CQL Equivalence

The goal of the __utilgen.py__ is to execute experiments to analyze the utility of the operators.
This tool execute experiments using the following combinations of operators:
- __SEQ__ / __BESTSEQ__;
- __SEQ__ / __CONSEQ__ / __BESTSEQ__;
- __SEQ__ / __CONSEQ__ / __ENDSEQ__ / __BESTSEQ__;
- __SEQ__ / __CONSEQ__ / __ENDSEQ__ / __MINSEQ__ / __MAXSEQ__ / __BESTSEQ__.
During the experiments execution the tool takes informations about the sequences sent to __BESTSEQ__ operator and about the comparisons performed by this operator.

# Parameters

The experiments parameters must be updated directly in the source code. The available parameters are the following:
- __ATT__: Number of attributes;
- __NSQ__: Number of distinct sequences;
- __RAN__: Temporal range;
- __SLI__: Slide interval;
- __PCT__: Percentage of consecutive instants (used only by __conseqgen.py__);
- __MAX__: Maximum valid length (used only by __maxseqgen.py__);
- __MIN__: Maximum valid length (used only by __minseqgen.py__);
- __RUL__: Number of rules (used only by __bestseqgen.py__);
- __LEV__: Maximum preference level (used only by __bestseqgen.py__);
- __IND__: Number of indifferent attributes (used only by __bestseqgen.py__).

Every parameter is dictionary with the keys __VAR__ (list of values) and __DEF__ (default parameter).

# Command Line

Despite StreamPrefGen is composed by many generators, all of them share the same command line options.

```
gen.py [-h] [-g] [-o] [-r] [-s]
  -h, --help       show the help message and exit
  -g, --gen        Generate files
  -o, --output     Generate query output
  -r, --run        Run experiments
  -s, --summarize  Summarize results
```
