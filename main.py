import upload
import luigi
import cats


class MainTask(luigi.WrapperTask):
    date = luigi.Parameter()

    def requires(self):
        analytics1 = cats.CatsAnalytics(self.date, 1)
        analytics3 = cats.CatsAnalytics(self.date, 3)

        # cascade upload
        upload1 = upload.UploadTable(self.date, '%s/cats/users1' % self.date,
                                     required_tasks=[analytics1])
        upload2 = upload.UploadTable(self.date, '%s/cats/popularity1' % self.date,
                                     required_tasks=[analytics1, upload1])
        upload3 = upload.UploadTable(self.date, '%s/cats/users3' % self.date,
                                     required_tasks=[analytics3, upload2])
        upload4 = upload.UploadTable(self.date, '%s/cats/popularity3' % self.date,
                                     required_tasks=[analytics3, upload3])

        return [upload.CatsReport(self.date),
                upload1, upload2, upload3, upload4]


if __name__ == '__main__':
    luigi.build([MainTask('2016-09-23')], workers=5)

