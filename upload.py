import luigi

import cats
# import dogs
import mr
import time
import os

UPLOAD_STATUS_PATH = 'upload/status'


class UploadTable(luigi.Task):
    date = luigi.Parameter()
    table = luigi.Parameter()
    required_tasks = luigi.Parameter(significant=False)

    def requires(self):
        return self.required_tasks

    def run(self):
        print('===UPLOAD STARTED: %s===' % self.table)
        for r in mr.read_table(self.table):
            # print slowly :)
            print('> %s' % r)
            time.sleep(1)

        # keep status in file
        UploadTarget.done(self.table)

    def output(self):
        return [UploadTarget(self.table)]


class CatsReport(luigi.Task):
    date = luigi.Parameter()

    def requires(self):
        return [cats.CatsAnalytics(self.date, 3)]

    def run(self):
        report_dir = '%s/report/' % self.date
        mr.mkdir(report_dir)

        cats_stats_table = '%s/cats/popularity3' % self.date
        mr.run_sort(cats_stats_table, report_dir + 'cats', sort_by='count', desc=True)

        top_10_cats = [r['kind'] for r in mr.read_table(report_dir + 'cats')][-10:]

        time.sleep(5)
        top_10_cats_recs = [{'kind': k, 'rating': len(top_10_cats) - idx}
                            for idx, k in enumerate(top_10_cats)]
        mr.write_table(top_10_cats_recs, report_dir + 'report')

    def output(self):
        return [mr.MrTarget('%s/report/report' % self.date)]


class UploadTarget(luigi.Target):

    def __init__(self, table):
        self.table = table

    def exists(self):
        status_rec = self.table + '\n'
        if os.path.exists(UPLOAD_STATUS_PATH):
            with open(UPLOAD_STATUS_PATH) as f:
                return status_rec in f.readlines()
        return False

    @staticmethod
    def done(table):
        if not os.path.exists('upload'):
            os.mkdir('upload')

        with open(UPLOAD_STATUS_PATH, 'a') as f:
            f.write(table + '\n')
