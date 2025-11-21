# DHealth-Distributed-Data-Processing-using-data-partitioning-techniques
## Abstract
This research investigates the performance of different data partitioning strategies for distributed big data processing using Apache Spark. The study evaluates hash-based, range-based, directory-based, and hybrid partitioning methods in both single-node and multi-worker cluster environments.

Experiments were conducted using a large personal health dataset, focusing on three aspects:

Execution Time vs. Number of Partitions

Execution Time Stability Across Multiple Runs

Hybrid Partitioning Performance (Range–Range, Hash–Hash, Range–Hash)

### Key Findings

Directory-based partitioning performs best in single-node setups due to efficient file pruning.

In cluster environments, directory methods introduce metadata overhead, reducing performance.

Range–Range hybrid partitioning consistently provides the best scalability and execution time in multi-worker clusters.

Range–Hash performs moderately well, while Hash–Hash and standard hash-based approaches suffer from shuffle overhead and skew.

## Project Overview
### Objective

To identify the most effective partitioning strategy for scalable distributed processing of large-scale wearable health data.

### Scope

Evaluate Hash, Range, Directory, and Hybrid partitioning techniques

Compare performance across single-node and distributed Spark clusters

Analyse workload balance, shuffle overhead, and execution time

Use Docker-based reproducible Apache Spark environment

## Methodology
### Partitioning Techniques

Hash Partitioning – Even distribution, high shuffle cost

Range Partitioning – Good for range queries, possible skew

Directory Partitioning – Efficient in single-node, heavy metadata in clusters

Hybrid Approaches:

Range–Range

Hash–Hash

Range–Hash

### Dataset

10,000+ records of synthetic and real wearable health attributes:

Heart Rate

Blood Oxygen Level

Sleep Duration

User ID

### Cluster Setup

Single-node Spark

Distributed Spark cluster (1 Master, 2 Workers) using Docker & Spark Standalone

## Experimental Summary
### Experiment 1 – Partitions vs. Execution Time

Directory-based → fastest in single-node

Range-based → stable in both setups

Hash-based → suffers from overhead due to frequent shuffles

### Experiment 2 – Stability Across Runs

Directory-based → stable in single-node

Range-based → improves after initial caching

Hash-based → fluctuates due to shuffle costs

### Experiment 3 – Hybrid Partitioning
#### Single Node

Directory-based → best

Range–Hash → overhead due to unnecessary double processing

#### Multi Worker

Range–Range → best

Hash–Hash → good balance, but overhead in initial runs

Range–Hash → highest initial overhead, but stabilizes

## Conclusion

Best for single-node:
✔ Directory-based partitioning

Best for distributed clusters:
✔ Range–Range hybrid partitioning
Provides optimal data balance, minimal skew, and improved parallelism.
