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

## Directory Structure
![image](https://github.com/rishav21556/K-Means-Clustering-with-MapReduce/assets/108609833/4cd68a26-4e0f-4f08-b564-e7e9b4f62760)

## Setup and Installation
1. Clone the repository to your local machine.
2. Ensure you have Python installed along with the necessary dependencies. You can install them using the following command:
pip install -r requirements.txt

To run the mapper.py script, follow these steps:
1. Open a terminal.
2. Navigate to the project directory.
3. Run the mapper.py script with the appropriate arguments. For example, to run mapper instances named "m1" and "m2":
python Master.py, python mapper.py m1, python mapper.py m2


Make sure to replace "m1" and "m2" with  "r1", and "r2" for running reducers.

## Additional Notes
- Ensure that you have the correct input data file in the specified format for the clustering algorithm to work properly.
- You may need to adjust the parameters such as the number of map tasks, reduce tasks, centroids, and iterations based on your specific use case.
- Monitor the execution progress by checking the logs generated during the execution.


