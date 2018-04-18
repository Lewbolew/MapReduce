from file_handler import FileHandler
from map_reduce import MapReduce
import multiprocessing
import config
import json
import os
class MapReduceManager(object):

    def __init__(self, input_dir, output_dir, num_mappers, num_reducers,
                 clean_splited_data=True):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers
        self.clean_splited_data = clean_splited_data
        self.clean = clean_splited_data
        self.map_reduce = MapReduce()
        self.file_handler = FileHandler(input_dir, output_dir)
        self.file_handler.split_file(self.num_mappers)

    def run_mapper(self, thread_id):
        key = None
        value = None
        with open(config.get_name_of_piece(thread_id), "r") as f:
            key = f.readline()
            value = f.read()
        if (self.clean_splited_data): os.unlink(config.get_name_of_piece(thread_id))
        mapper_result = self.map_reduce.mapper(key, value)
        shuffled_result = self.map_reduce.shuffler(mapper_result)
        with open(config.get_map_file_name(), "w") as f:
            json.dump()


    def run_reducer(self, thread_id):
        pass

    def run(self):
        mappers = list()
        reducers = list()
        for thread_id in range(self.num_mappers):
            new_mapper = multiprocessing.Process(target=self.run_mapper, args=(thread_id,))
            new_mapper.start()
            mappers.append(new_mapper)
        list(map(lambda x: x.join(), mappers))
        for thread_id in range(self.num_reducers):
            new_reducer = multiprocessing.Process(target=self.run_reducer, args=(thread_id,))
            new_reducer.start()
            reducers.append(new_reducer)
        list(map(lambda x: x.join(), reducers))




