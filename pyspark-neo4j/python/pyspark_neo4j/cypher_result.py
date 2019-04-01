from pyspark.sql import DataFrame

class CypherResult(object):

    def __init__(self, jCypherResult, sparkCypherSession):
        self._jCypherResult = jCypherResult
        self._sparkCypherSession = sparkCypherSession
        self._sqlContext = sparkCypherSession.sparkSession._wrapped
        self.__df = None

    @property
    def df(self):
        if self.__df is None:
            self.__df = DataFrame(self._jCypherResult.df(), self._sqlContext)
        return self.__df
