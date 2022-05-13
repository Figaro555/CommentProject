from pyspark.sql import DataFrame
from pyspark.sql.functions import col, max, current_timestamp


class DataFrameAggregator:
    def aggregate(self, data_frame: DataFrame, limit: int, column_to_compare: str):
        result = data_frame.groupBy('id', 'videoId') \
            .agg(max(column_to_compare).alias(column_to_compare),
                 max('text').alias('text')
                 ) \
            .limit(limit) \
            .sort(col(column_to_compare).desc())\
            .withColumn("time1", current_timestamp())

        result.show()

        return result


