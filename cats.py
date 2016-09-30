import datetime

import luigi

import mr
import time


class ImportCatsLog(luigi.Task):
    date = luigi.Parameter()

    def parse_log(self, rec):
        print(rec)
        if rec['action'] == 'like':
            yield {'user': rec['user'], 'kind': rec['kind']}

    def requires(self):
        return [mr.ExternalMrInput('logs/cats/%s' % self.date)]

    def run(self):
        mr.mkdir('%s/cats' % self.date)

        log_table = 'logs/cats/%s' % self.date
        likes_table = '%s/cats/likes' % self.date

        mr.run_map(self.parse_log, log_table, likes_table)
        time.sleep(20)

    def output(self):
        return [mr.MrTarget('%s/cats/likes' % self.date)]


class CatsAnalytics(luigi.Task):
    date = luigi.Parameter()
    days = luigi.IntParameter()

    def __init__(self, date, days):
        self.cats_dir = '%s/cats/' % date
        self.popularity_table = self.cats_dir + 'popularity%d' % days
        self.user_activity_table = self.cats_dir + 'users%d' % days
        super(CatsAnalytics, self).__init__(date, days)

    def days_before(self, date_str):
        base = datetime.datetime.strptime(date_str, '%Y-%m-%d')
        date_list = [base - datetime.timedelta(days=x) for x in range(self.days)]
        return [dt.strftime('%Y-%m-%d') for dt in date_list]

    def requires(self):
        return [ImportCatsLog(dt) for dt in self.days_before(self.date)]

    def run(self):
        mr.mkdir(self.cats_dir)

        day_tables = ['%s/cats/likes' % dt for dt in self.days_before(self.date)]

        mr.count_recs_by_column(day_tables, self.popularity_table,
                                column='kind', count_col='count')

        mr.count_recs_by_column(day_tables, self.user_activity_table,
                                column='user', count_col='count')
        time.sleep(10 * self.days)

    def output(self):
        return [mr.MrTarget(self.popularity_table),
                mr.MrTarget(self.user_activity_table)]
