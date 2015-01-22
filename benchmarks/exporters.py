import csv


class ExporterBase(object):
    def __init__(self):
        pass

    def export(self, data):
        raise NotImplemented

class Csv(ExporterBase):
    def __init__(self, csvfile):
        self._file = csvfile
        self._writer = None

    def export(self, data):
        if self._writer is None:
            self._writer = csv.DictWriter(self._file, fieldnames=data[0].keys())
            self._writer.writerow(dict((fn,fn) for fn in self._writer.fieldnames))
            #writer.writeheader()
        self._writer.writerows(data)

