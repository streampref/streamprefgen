**Table of Contents**

- [Introduction](#introduction)

# Introduction

StreamPrefGen is a dataset generator for evaluation of operators of StreamPref query language.
The operators are implemented in the [StreamPref](http://streampref.github.io) a prototype of Data Stream Management System (DSMS).

# Individual Generators

The StreamPrefGen is composed by individual dataset generators for evaluation of specific StreamPref operators.

## SeqGen

SeqGen is the generator for evaluation of SEQ operator (sequence extraction).

# Command Line

Despite StreamPrefGen is composed by many generators.
All of them share the same command line options:
- -h/--help: Show the help message
- -g/--gen: Generate files
- -o/--output: Configure files to generate query results
- -r/--run: Run experiments
- -s/--summarize: Summarize experiments results
