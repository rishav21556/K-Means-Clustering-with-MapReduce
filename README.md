# K-Means Clustering with MapReduce

## Overview
This project implements the K-Means clustering algorithm using the MapReduce framework. It partitions a dataset into K clusters using an iterative algorithm executed in a distributed manner.
## Setup
- Deployment of MapReduce framework on a single machine
- Each mapper, reducer, and master is a separate process
- Use of gRPC for communication

## Implementation Details
### Components
- Master: Controls and communicates with other components.
- Mapper: Processes input data split and applies the Map function.
- Reducer: Receives and processes intermediate data, applies Reduce function.
- Input Split: Divides input data into smaller chunks for parallel processing.
- Partition: Divides Mapper output into smaller partitions for shuffling and sorting.
- Shuffle and Sort: Sorts intermediate key-value pairs and groups values by key.
- Centroid Compilation: Compiles final centroids from Reducer output.

### Steps
1. **Initialization:** Randomly initialize K cluster centroids.
2. **Map Phase:** Apply Map function to each input split, generate intermediate key-value pairs.
3. **Partition Phase:** Partition Mapper output into smaller partitions.
4. **Shuffle and Sort Phase:** Sort intermediate key-value pairs by key.
5. **Reduce Phase:** Apply Reduce function to each group of values, generate final output.
6. **Centroid Compilation:** Parse Reducer output to compile final centroids.

### Fault Tolerance
- Handle failures associated with Mapper or Reducer.
- Re-run failed tasks to ensure computation completion.

## Directory Structure (Sample)
Data/
├─ Input/
│  ├─ points.txt (initial points)
├─ Mappers/
│  ├─ M1/
│  │  ├─ partition_1.txt
│  │  ├─ partition_2.txt
│  │  ├─ partition_R.txt
│  ├─ M2/ ...
│  ├─ M3/ ...
...
├─ Reducers/
│  ├─ R1.txt
│  ├─ R2.txt
├─ centroids.txt (final list of centroids)

