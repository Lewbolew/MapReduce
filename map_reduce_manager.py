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
        slice_index = 0
        stride = len(mapper_result) // self.num_reducers
        for reducer_index in range(self.num_reducers):
            with open(config.get_map_file_name(thread_id, reducer_index), "w+") as f:
                json.dump([(key, value) for (key, value) in
                            mapper_result[slice_index: slice_index + stride]],f)
                slice_index += stride



    def run_reducer(self, thread_id):
        key_value_dict = {}
        for mapper_index in range(self.num_mappers):
            curr_map_json = None
            with open(config.get_map_file_name(mapper_index, thread_id)) as f:
                curr_map_json = json.load(f)
            for (key, value) in curr_map_json:
                if key in key_value_dict:
                    key_value_dict[key].append(value)
                else:
                    key_value_dict[key] = [value]
            if self.clean_splited_data: os.unlink(config.get_map_file_name(mapper_index, thread_id))
        result = [self.map_reduce.reducer(key, key_value_dict[key]) for key in key_value_dict]
        with open(config.get_reduce_result_file_name(thread_id), 'w+') as f:
            json.dump(result, f)

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




