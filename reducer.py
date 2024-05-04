

import pandas as pd
import os
import sys
import grpc
from map_reduce_pb2 import *
import map_reduce_pb2_grpc as map_reduce_grpc
from concurrent import futures
import time
from grpc._channel import _InactiveRpcError
import ast
import numpy as np
import random
from hashport import hashport


def data_point_format(data):
    data_points = []
    for index, row in data.iterrows():
        data_points.append(data_point(x=row[0], y=row[1]))
    return data_points

def custom_sort_cond(data_point):
    return data_point.key

def probab_flag(probability):
    # Generate a random number from a binomial distribution
    flag = random.random() < probability
    return bool(flag)

class Reduce(map_reduce_grpc.ReducerServicer):
    def __init__(self):
        self.fetched_data = []
        self.partition_index = 0
    
    def is_alive(self, request, context):
        return is_alive_response(alive=True)

    def reducer_assign_task(self, request, context):
        print("-----------------Reducer Assign Task-----------------")
        self._id  = request.id
        self.partition_index = request.partition_index
        self.M = request.M
        self.R = request.R
        self.k = request.k
        self.mapper_port = request.mapper_port
        self.num_mappers = 2
        self.num_reducers = 2
        self.num_mappers_name = ["m1","m2"]
        self.num_reducers_name = ["r1","r2"]

        self.fetched_data = []

        print(f"Reduce task {self.partition_index}")

        print(f"Reduce Task {self.partition_index}")
        with open("Data/Reducers/R"+str(self.partition_index)+".txt", "w") as f:
            f.write(f"key,x,y\n")
        # doing fetching data from the mappers (shuffle)

        
        for i in range(self.num_mappers):
            ret = self.fetch(self.mapper_port[i])
        
        # sorting the fetched data
        # print(self.fetched_data)
        if (checkFlag == 1):
            print(">> Press ctrl+c to stop the mapper and check fault tolerance")
            time.sleep(10)
            print(">> Time finished")
        
        if (len(self.fetched_data) == 0):
            print("No data to reduce")
            print(f"partition index {self.partition_index}")
            return master_to_reducer_task_assign_response(success=True)
        

        self.fetched_data = sorted(self.fetched_data, key=lambda item: item['key'])
        self.reduce()

        if (probab_flag(0.7)):
            return master_to_reducer_task_assign_response(success=True)
        return master_to_reducer_task_assign_response(success=False)
        # return master_to_reducer_task_assign_response(success=True)
    
    def fetch(self,port):
        
        channel = grpc.insecure_channel('127.0.0.1:'+str(port))
        stub = map_reduce_grpc.MapperStub(channel)
        print(port,self.partition_index)
        response = stub.give_partition_data(reducer_to_mapper_file_read(partition_index=self.partition_index,reducer_id = self._id))
        
        if (response.success):
            print(f"reducer {self._id} fetched data from mapper {port}")
            data = response.data_points
            if (len(data) != 0):
                dir = []
                for i in range(len(data)):
                    dir.append({'key':data[i].key,'x':data[i].value.x, 'y': data[i].value.y ,'count':data[i].count})
                data = dir
            self.fetched_data += data
            return True
        else:
            print('Error in fetching data')
            return False

    def reduce(self):
        # now we have the data in self.fetched_data
        # we will now reduce it
        
        length = len(self.fetched_data)
        if (length == 0):
            print("No data to reduce")
            return 
        
        idx = 0
        cnt = 0
        point = data_point(x=0, y=0)
        while(idx < length):
            k = self.fetched_data[idx]['key']
            point.x = self.fetched_data[idx]['x']
            point.y = self.fetched_data[idx]['y']
            cnt = self.fetched_data[idx]['count']
            idx += 1
            
            if (idx == length):
                break
            while(self.fetched_data[idx]['key'] == k):
                point.x += self.fetched_data[idx]['x']
                point.y += self.fetched_data[idx]['y']
                cnt += self.fetched_data[idx]['count']
                k = self.fetched_data[idx]['key']
                idx += 1
                if (idx == length):
                    break
            print("key : ",k)
            with open("Data/Reducers/R"+str(self.partition_index)+".txt", "a") as f:
                f.write(str(k) + "," + str(point.x/cnt) + "," + str(point.y/cnt) + "\n")
            
            point.x = 0
            point.y = 0
            cnt = 0

checkFlag = int(input("checkFlag : "))
Name = sys.argv[1]
port = hashport(Name)
print(f"reducer-{port} pid : ",os.getpid())
print(f"Reducer is running on the port : {port}")
server = grpc.server(futures.ThreadPoolExecutor(max_workers=500))
map_reduce_grpc.add_ReducerServicer_to_server(Reduce(),server)
server.add_insecure_port(f"127.0.0.1:{port}")
server.start()
server.wait_for_termination()