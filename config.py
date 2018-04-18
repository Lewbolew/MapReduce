FILE_NAME = 'file.ext'



def get_name_of_piece(thread_id, ext='.piece'):
    return FILE_NAME.split('.')[0] + str(thread_id) + ext

def get_map_file_name():
    pass