
import pandas as pd
import os
import sys
import grpc
from map_reduce_pb2 import *
import map_reduce_pb2_grpc as map_reduce_grpc
from concurrent import futures
import time
import numpy as np
import random
from hashport import hashport

# PORT = int(sys.argv[1])
END_SERVER = False
def data_point_format(data):
    data_points = []
    for index, row in data.iterrows():
        data_points.append(data_point(x=row[0], y=row[1]))
    return data_points
def convert_to_mapper_to_reducer_data_point(data):
    data_points = []
    for index, row in data.iterrows():
        # print(row)
        data_points.append(mapper_to_reducer_data_point(key = int(row[0]), value = data_point(x=float(row[1]), y=float(row[2])), count=int(row[3])))
    return data_points

def probab_flag(probability):
    # Generate a random number from a binomial distribution
    flag = random.random() <= probability
    return bool(flag)

class Mapper(map_reduce_grpc.MapperServicer):
    def __init__(self):
        self.id = 0

    def is_alive(self, request, context):
        return is_alive_response(alive=True)
    
    def assign_task(self, request, context):
        self.start = request.start_index
        self.end = request.end_index
        self.k_clusters = request.k_clusters
        self.data_points = request.data_points
        self.id = request.id
        self.M = request.M
        self.R = request.R
        self.k = request.k
        self.map_task = request.map_task
        self.num_mappers = 2
        self.num_reducers = 2
        self.num_mappers_name = ["m1","m2"]
        self.num_reducers_name = ["r1","r2"]

        input_file_path = "Data/Input/points.txt"

        # reading the input file 
        data = pd.read_csv(input_file_path, header=None)
        self.data_points = data_point_format(data[self.start:self.end])

        # now creating corresponding directory for this mapper
        print(f"Mapper {self.map_task} is running")
        try:
            os.mkdir(f"Data/Mappers/M{self.map_task}")
        except FileExistsError:
            pass

        
        for i in range(self.R):
            file_path = f"Data/Mappers/M{self.map_task}/Partition_{i}.txt"
            mode = 'w'
            with open(file_path, mode) as f:
                f.write("key,x,y,count\n")
        
        if (checkFlag == 1):
            print("Press ctrl+c to stop the mapper and check fault tolerance")
            time.sleep(10)
            print("time finished")
        
        if (probab_flag(0.5)):
            return master_to_mapper_task_assign_response(success=False)
        
        self.map(self.data_points, self.k_clusters)
        
        
        return master_to_mapper_task_assign_response(success=True)
    
    def euclidean_distance(self, data_point, k_cluster):
        return ((data_point.x - k_cluster.x) ** 2 + (data_point.y - k_cluster.y) ** 2) ** 0.5
    
    def map(self, data_points, k_clusters):
        for i in range(len(data_points)):
            min_dist = float('inf')
            cluster = None
            for j in range(len(k_clusters)):
                dist = self.euclidean_distance(data_points[i], k_clusters[j])
                if dist < min_dist:
                    min_dist = dist
                    cluster = j
            self.emit(cluster, data_points[i])

    def partition_function(self,key):
        return key % self.R
    
    def emit(self, cluster, data_point):
        partition = self.partition_function(cluster)
        with open(f"Data/Mappers/M{self.map_task}/Partition_{partition}.txt", "a") as f:
            # print(self.map_task,partition,cluster)
            f.write(f"{cluster+1},{data_point.x}, {data_point.y},{1}\n")

    def give_partition_data(self,request,context):
        # return reducer_to_mapper_file_read_response(data_points = [], success=True)
        partition_index = request.partition_index
        # print(f"Partition {partition_index} requested from Mapper {self.id}")
        ret = []
        for idx in range(self.id,self.M,self.num_mappers):
            try:
                data_ = pd.read_csv(f"Data/Mappers/M{idx}/Partition_{partition_index}.txt", header=None)
            except pd.errors.EmptyDataError as e:
                continue
            except Exception as e:
                continue

            data_points = convert_to_mapper_to_reducer_data_point(data_.iloc[1:,:])
            ret += data_points

        return reducer_to_mapper_file_read_response(data_points = ret, success=True)

checkFlag = int(input("checkFlag : "))

Name = sys.argv[1]
port = hashport(Name)
print(f"mapper-{port} pid : ",os.getpid())
print(f"Mapper is running on the port : {port}")
server = grpc.server(futures.ThreadPoolExecutor(max_workers=500))
map_reduce_grpc.add_MapperServicer_to_server(Mapper(),server)
server.add_insecure_port(f"127.0.0.1:{port}")
server.start()
server.wait_for_termination()
print("Mapper Terminated")