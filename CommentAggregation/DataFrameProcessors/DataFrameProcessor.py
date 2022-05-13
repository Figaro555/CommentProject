from DataFrameProcessors.DataFrameCreator import DataFrameCreator


class DataFrameProcessor:
    def process_df(self, rdd, agg_column, ss, path_to_save, limit):
        df_creator = DataFrameCreator()


        try:
            df = df_creator.create_df(rdd, agg_column, ss)




        except Exception as _ex:
            print(_ex)
