import json

from Transformers.YouTubeTransformer import YouTubeTransformer
from pyspark.streaming import StreamingContext, DStream

from CommentSender.Extractors import KinesisExtractor


class Executor:
    def execute(self, path_to_save, limit, ss, window_size):
        kinesis_ex = KinesisExtractor()
        yt_transformer = YouTubeTransformer()


        sc = ss.sparkContext
        ssc = StreamingContext(sc, 1)

        stream: DStream = kinesis_ex.get_data_stream(ssc, window_size, window_size)
        stream.pprint()

        stream = stream.map(lambda comment: json.loads(comment))
        stream.pprint()

        like_stream = stream.map(lambda comment: yt_transformer.like_transformer(comment))
        like_stream.pprint()

        length_stream = stream.map(lambda comment: yt_transformer.length_transformer(comment))
        length_stream.pprint()

        reply_stream = stream.map(lambda comment: yt_transformer.reply_transformer(comment))
        reply_stream.pprint()



        ssc.start()
        ssc.awaitTermination()
