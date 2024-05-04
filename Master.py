import pandas as pd
import os
import grpc
from map_reduce_pb2 import *
import map_reduce_pb2_grpc as map_reduce_grpc
from concurrent import futures
import time
from hashport import hashport
import multiprocessing
# from mapper import Mapper
# from reducer import Reduce
import threading
import traceback

import random
print("Master pid : ",os.getpid())

class CountDownTimer :
    def __init__(self, MAX_T, type=['random', 'fixed']):
        self.max_t = MAX_T
        self.type = type
        self.Timer = 0

    def countDown(self, timeout=None, annotation=""):
        self.restart_timer(timeout)

        while self.Timer > 0:
            print(f"{self.Timer} {annotation}")
            time.sleep(1)
            self.Timer -= 1
        return 0
    
    def restart_timer(self):
        self.Timer = self.max_t


def data_point_format(data):
    data_points = []
    for index, row in data.iterrows():
        data_points.append(data_point(x=row[0], y=row[1]))
    return data_points

def pick_index_with_one(lst):
    # Filter the indices where the value is 1
    one_indices = [i for i, val in enumerate(lst) if val == 1]
    
    # Randomly select one index from the filtered list
    random_index = random.choice(one_indices) if one_indices else None
    return random_index

def delete_files_in_directory(directory):
    for item in os.listdir(directory):
        item_path = os.path.join(directory, item)
        if os.path.isfile(item_path):
            # If it's a file, remove it
            os.remove(item_path)
        elif os.path.isdir(item_path):
            # If it's a directory, recursively call the function to delete its contents
            delete_files_in_directory(item_path)
            # After deleting contents, remove the directory itself
            os.rmdir(item_path)

print("MASTER IS RUNNING ...")
M = int(input("Number of Mappers (M) : "))
R = int(input("Number of Reducers (R) : "))
Centroids = int(input("Number of Centroids (K) : "))
Iterations = int(input("Number of Iterations : "))

print("------------------------------------")

input_file_path = "Data/Input/points.txt"

# reading the input file 
data = pd.read_csv(input_file_path, header=None)

class Master():
    def __init__(self,M,R,k,Iterations,data):
        delete_files_in_directory("Data/Mappers")
        delete_files_in_directory("Data/Reducers")
        self.M = M
        self.R = R
        self.k = k
        self.Iterations = Iterations
        self.itr = Iterations
        self.data = data
        self.num_mappers = 2
        self.num_reducers = 2
        self.num_mappers_name = ["m1","m2"]
        self.num_reducers_name = ["r1","r2"]

        """randomly selecting k points as centroids"""
        self.centeroids = self.data.sample(n=self.k)
        
        """converting data point to appropriate format"""
        self.data = data_point_format(self.data)
        self.centeroids = data_point_format(self.centeroids)

        self.mapperList = [0 for i in range(self.num_mappers)]
        self.mapperPort = [ hashport(self.num_mappers_name[i]) for i in range(self.num_mappers)]
        self.reducerList = [0 for i in range(self.R)]
        self.reducerPort = [hashport(self.num_reducers_name[i]) for i in range(self.num_reducers)]

        # lists for fault tolerance
        self.active_mappers = [0 for i in range(self.num_mappers)]
        self.mappers_completed = [0 for i in range(self.M)]

        self.active_reducers = [0 for i in range(self.num_reducers)]
        self.reducers_completed = [0 for i in range(self.R)]

        self.assign_tasks_to_mappers()

    def send_heart_beat_mapper(self,i):
        try:
            channel = grpc.insecure_channel(f'127.0.0.1:{self.mapperPort[i]}')
            stub = map_reduce_grpc.MapperStub(channel)
            response = stub.is_alive(is_alive_response(alive=True))
            if (response.alive):
                # print("The node is active")
                self.active_mappers[i] = 1
        except Exception as e:
            self.active_mappers[i] = 0

    def send_heart_beat_reducer(self,i):
        try:
            channel = grpc.insecure_channel(f'127.0.0.1:{self.reducerPort[i]}')
            stub = map_reduce_grpc.ReducerStub(channel)
            response = stub.is_alive(is_alive_response(alive = True))
            if (response.alive):
                # print("The node is active")
                self.active_reducers[i] = 1
        except Exception as e:
            self.active_reducers[i] = 0
        
    def heart_beat(self):
        while True:
            time.sleep(0.5)
            for i in range(self.num_mappers):
                t1 = threading.Thread(target=self.send_heart_beat_mapper, args=(i,))
                t1.start()
                t2 = threading.Thread(target=self.send_heart_beat_reducer, args=(i,))
                t2.start()
    
    def thread_assign_map(self,map_task,i,start_index,end_index,port_index):
        try:
            channel = grpc.insecure_channel(f'127.0.0.1:{self.mapperPort[port_index]}')
            stub = map_reduce_grpc.MapperStub(channel)
            response = stub.assign_task(master_to_mapper_task_assign(start_index = start_index, end_index= end_index, k_clusters=self.centeroids, data_points=self.data[start_index:end_index], M = self.M, R = self.R, k = self.k,id = port_index,map_task = map_task))
            if (response.success):
                self.mappers_completed[map_task] = 1
                self.active_mappers[i] = 1
                print(f"Map task {map_task} done by mapper {i}")
            else :
                self.mappers_completed[map_task] = 0
                self.active_mappers[i] = 1
                print(f"Map task {map_task} was not done by mapper {i}")
                print(f"Reassigning map task {map_task} to the mapper {i}")
                self.thread_assign_map(map_task,i,start_index,end_index,port_index)
                

        except Exception as e:
            print(f"Mapper {i} is offline, {map_task} was not completed\n")
            # since previous mapper failed creating a new mapper
            self.mappers_completed[map_task] = 0
            self.active_mappers[i] = 0

     
    def thread_assign_reduce(self,reduce_task, i):
        try:
            channel = grpc.insecure_channel(f'127.0.0.1:{self.reducerPort[i]}')
            stub = map_reduce_grpc.ReducerStub(channel)
            # response1 = stub.is_alive(is_alive_response(alive = False))
            # print(i,response1)
            response = stub.reducer_assign_task(master_to_reducer_task_assign(partition_index = reduce_task, mapper_port = self.mapperPort, M = self.M, R = self.R, k = self.k, id = i))
            
            if (response.success):
                self.reducers_completed[reduce_task] = 1
                self.active_reducers[i] = 1
                print(f"Reduce task {reduce_task} done by reducer {i}")
            else:
                self.reducers_completed[reduce_task] = 0
                self.active_reducers[i] = 1
                print(f"Reduce task {reduce_task} was not done by reducer {i}")
                print(f"Reassigning task  {reduce_task} to the reducer {i}")
                self.thread_assign_reduce(reduce_task,i)
        except Exception as e:
            print(e)
            print(f"reducer {i} is not working offline, reduce task {reduce_task} was not completed\n")
            self.reducers_completed[reduce_task] = 0
            self.active_reducers[i] = 0

        
    def assign_tasks_to_mappers(self):
        
        if (self.Iterations==0):
            # writing the final answer in the file
            with open("centroids.txt", "w") as f:
                for index, rows in enumerate(self.centeroids):
                    f.write(f"{index+1},{rows.x},{rows.y}\n")
            print("Training Fininshed")
            sys.exit(0)
        
        # while ((0 in self.mappers_completed)):
        
        while((0 in self.mappers_completed)):
            print(f"Running mapper in {2} seconds")
            
            time.sleep(2)
            for i in range(0,self.M,self.num_mappers):
                map_threads = []
                for j in range(self.num_mappers):
                    if (i==self.M):
                        break
                    if (self.mappers_completed[i]==True):
                        continue
                    print(f"Starting map task {i+1} ")
                    start_index = i*(len(self.data)//self.M)
                    end_index = (i+1)*(len(self.data)//self.M)
                    map_thread = threading.Thread(target = self.thread_assign_map, args=(i,i%self.num_mappers,start_index,end_index,i%self.num_mappers))
                    map_threads.append(map_thread)
                    map_thread.start()
                    i += 1
                for thread in map_threads:
                    thread.join()
                
                if (0 not in self.mappers_completed):
                    break
                i -= 1
            print("Mappers threads joined")


        self.assign_task_to_reducer()

    
    def assign_task_to_reducer(self):

        # while ((0 in self.mappers_completed)):
        
        while((0 in self.reducers_completed)):
            print(f"Running reducer in {2} seconds")
            time.sleep(2)
            
            for i in range(0,self.R,self.num_reducers):
                reduce_threads = []
                for j in range(self.num_reducers):
                    if (i==self.R):
                        break
                    if (self.reducers_completed[i]==True):
                        continue
                    reduce_thread = threading.Thread(target = self.thread_assign_reduce, args=(i, i%self.num_reducers,))
                    reduce_threads.append(reduce_thread)
                    reduce_thread.start()
                    i += 1
                for thread in reduce_threads:
                    thread.join()
                if (0 not in self.reducers_completed):
                    break
                i -= 1
            print("Reducer threads joined")

        print(f"-----------Iteration {self.itr - self.Iterations} completed-----------")
        self.Iterations -= 1
        # lists for fault tolerance
        self.active_mappers = [0 for i in range(self.num_mappers)]
        self.mappers_completed = [0 for i in range(self.M)]

        self.active_reducers = [0 for i in range(self.num_reducers)]
        self.reducers_completed = [0 for i in range(self.R)]

        self.assign_tasks_to_mappers()

        
            



import sys


try:
    Master(M,R,Centroids,Iterations,data)
except KeyboardInterrupt as e:
    sys.exit(0)


# python3 -m grpc_tools.protoc -I . --python_out=. --pyi_out=. --grpc_python_out=. map_reduce.proto
# sudo apt clean
# sudo apt autoclean
# sudo apt-get clean
# sudo apt-get autoclean