import luigi
import os
from itertools import chain, groupby
import types

MR_DIR = 'mr/'


def count_recs_by_column(in_tables, out_table, column, count_col=None):
    def count_reducer(key, recs):
        recs_count = sum(1 for r in recs)
        yield {column: key, count_col: recs_count}

    run_reduce(count_reducer, in_tables, out_table, reduce_by=column)


def run_map(map_func, input_tables, output_table):
    recs = _read_tables(input_tables)
    write_table(flatten(map(map_func, recs)), output_table)


def _reduce_by_key(reduce_func, reduce_by, recs):
    sorted_recs = sorted(recs, key=lambda rec: rec[reduce_by])
    for reduce_key, grouped_recs in groupby(sorted_recs,
                                            key=lambda rec: rec[reduce_by]):
        for r in reduce_func(reduce_key, grouped_recs):
            yield r


def run_reduce(reduce_func, input_tables, output_table, reduce_by):
    recs = _read_tables(input_tables)
    reduce_output = _reduce_by_key(reduce_func, reduce_by, recs)
    write_table(reduce_output, output_table)


def run_sort(input_tables, out_table, sort_by, desc=False):
    sorted_recs = sorted(_read_tables(input_tables), key=lambda rec: int(rec[sort_by]), reverse=desc)
    write_table(sorted_recs, out_table)


def read_table(table):
    with open(MR_DIR + table) as f:
        for line in f.readlines():
            out_rec = dict()
            fields = line.strip('\n').split(';')
            for field in fields:
                key, value = field.split('=')
                out_rec[key] = value
            yield out_rec


def _read_tables(tables):
    return chain.from_iterable(read_table(t) for t in flatten(tables))


def write_table(stream, table):
    with open(MR_DIR + table, 'w') as f:
        for rec in stream:
            line = ';'.join(str(k) + '=' + str(v) for k, v in rec.iteritems())
            f.write(line + '\n')


def mkdir(dir_name):
    if not os.path.exists(MR_DIR + dir_name):
        os.makedirs(MR_DIR + dir_name)


def flatten(obj, list_types=(list, tuple, set, types.GeneratorType)):
    """ Create flat list from all elements. """
    if isinstance(obj, list_types):
        return list(chain(*map(flatten, obj)))
    return [obj]


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
