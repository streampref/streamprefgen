**Table of Contents**

- [Introduction](#introduction)
- [Generators](#generators)
- [Command Line](#command-line)

# Introduction

StreamPrefGen is a dataset generator for evaluation of operators of StreamPref query language.
The operators are implemented in the [StreamPref](http://streampref.github.io) a prototype of Data Stream Management System (DSMS).

# Generators

The StreamPrefGen is composed by individual dataset generators for evaluation of specific StreamPref operators.

- **SeqGen**: generator for evaluation of SEQ operator (sequence extraction);
- **TPref**: generator for evaluation of BESTSEQ operator (temporal preference operator).

# Command Line

Despite StreamPrefGen is composed by many generators.
All of them share the same command line options:
- -h/--help: Show the help message
- -g/--gen: Generate files
- -o/--output: Configure files to generate query results
- -r/--run: Run experiments
- -s/--summarize: Summarize experiments results
