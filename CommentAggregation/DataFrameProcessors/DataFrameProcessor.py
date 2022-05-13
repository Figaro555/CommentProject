from DataFrameProcessors.DataFrameAggregator import DataFrameAggregator
from DataFrameProcessors.DataFrameCreator import DataFrameCreator


class DataFrameProcessor:
    def process_df(self, rdd, agg_column, ss, path_to_save, limit):
        df_creator = DataFrameCreator()
        df_aggregator = DataFrameAggregator()

        try:
            df = df_creator.create_df(rdd, agg_column, ss)

            df_agg = df_aggregator.aggregate(df, limit, agg_column[0])

            df_agg.write.mode('append').partitionBy("time1").json(
                path_to_save + "/" + "/Aggregated/" + agg_column[0])


        except Exception as _ex:
            print(_ex)
