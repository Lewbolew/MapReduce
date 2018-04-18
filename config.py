FILE_NAME = 'file.ext'
DEFAULT_OUTPUT_DIR = 'input_data'
DEFAULT_INPUT_DIR = 'output_data'
MUPPERS_NUM = 3
REDUSERS_NUM = 3


def get_name_of_piece(thread_id, ext='.piece'):
    return FILE_NAME.split('.')[0] + str(thread_id) + ext

def get_map_file_name(thread_id, reducer, output_dir=DEFAULT_OUTPUT_DIR, ext='.piece'):
    return output_dir + FILE_NAME + str(thread_id) + '-' + str(reducer) + ext

def get_reduce_result_file_name(reducer, output_dir=DEFAULT_OUTPUT_DIR, ext='.res'):
    return output_dir + 'result_' + str(reducer) + ext