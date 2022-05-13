from pyspark.streaming import StreamingContext, DStream

from CommentSender.Extractors import KinesisExtractor


class Executor:
    def execute(self, path_to_save, limit, ss, window_size):
        kinesis_ex = KinesisExtractor()


        sc = ss.sparkContext
        ssc = StreamingContext(sc, 1)

        stream: DStream = kinesis_ex.get_data_stream(ssc, window_size, window_size)
        stream.pprint()

        ssc.start()
        ssc.awaitTermination()
