

class FileHandler(object):
    """
    Class for all file manipulations.
    """
    def __init__(self, data_dir, save_data_dir):
        """

        :param data_dir: the location of the original data
        :param save_data_dir: location where results should be saved
        """
        self.data_dir = data_dir
        self.save_data_dir = save_data_dir

    def split_file(self, num_splits):
        pass

    def join_files(self):
        pass
