# Overview

The iReceptor Turnkey repository and its components have been carefully optimized to perform a certain set of queries efficiently, even when a repository stores a very large amount of rearrangement data. This is done by both creating indexes for the fields that are commonly searched, but also by creating clever internal data structures within the repository that help to perform very specific searches very quickly.

Although the basic configuration of the iReceptor Platform and its repositories are configured to work efficiently "out of the box" for these queries, as anyone that uses computers knows, strange things can happen. Poor performance can be caused by a wide range of problems, including poorly configured repositories (e.g. incorrect or missing indexes), servers that are not configured appropriately for the size of the data being processed (e.g. not enough memory), or over subscription from users (e.g. too many people doing too many queries).

This directory contains some simple benchmarking code and scripts that can be used to assess and monitor the performance of your repository. The benchmarking scripts in this directory are targeted at helping you to catch and diagnose performance problems at the Mongo repository level. The benchmarks run through using the scripts in this directory perform a range of commonly used queries at the rearrangement level of the repository. The queries include queries for V/D/J Genes, AA Junction sequences, Junction Length across all of the samples in the repository.

Diagnostics are reported for these searchers, including:

1. The number of sequences found and the time it took to find them for each query and each sample.
1. For each query, the query, the indexes, and the type of query that was performed.
1. The indexes that exist on the repository at the time the queries were run.
1. The query plans (an optimization approach that mongo uses) that were in place at the time the queries were run.

Combined, these diagnostics can help you understand the performance of your repository, and in particular, if used regularly can help catch performance issues that might arise over time.


