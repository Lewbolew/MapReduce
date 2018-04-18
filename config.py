FILE_NAME = 'file.ext'



def get_map_file_name(thread_id, ext='.piece'):
    return FILE_NAME.split('.')[0] + str(thread_id) + ext