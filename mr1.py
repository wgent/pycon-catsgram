import luigi
import os

MR_DIR = 'mr/'


def run_map(map_func,
            input_tables, output_table):
    pass

def run_reduce(reduce_func,
               input_tables, output_table,
               reduce_by):
    pass

def run_sort(input_tables, out_table,
             sort_by, desc=False):
    pass


def read_table(table):
    pass

def write_table(stream, table):
    pass

def mkdir(dir_name):
    pass


class MrTarget(luigi.Target):
    def __init__(self, path):
        self.path = path

    def exists(self):
        mr_path = MR_DIR + self.path
        return os.path.exists(mr_path) and os.path.getsize(mr_path) > 0


class ExternalMrInput(luigi.ExternalTask):
    table = luigi.Parameter()

    def output(self):
        return MrTarget(self.table)
