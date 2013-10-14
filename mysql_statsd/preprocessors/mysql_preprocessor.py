from interface import Preprocessor


class MysqlPreprocessor(Preprocessor):
    def __init__(self, *args, **kwargs):
        super(MysqlPreprocessor, self).__init__(*args, **kwargs)

    def process(self, rows):
        return list(rows)
