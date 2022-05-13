class DataFrameCreator:
    def create_df(self, rdd, agg_column, ss):
        columns = ['id', 'videoId', 'text', 'date'] + agg_column
        try:
            df = ss.createDataFrame(rdd, columns)
            df.printSchema()
            df.show(truncate=False)
            return df
        except Exception as _ex:
            print(_ex)
