from pyspark.streaming.kinesis import InitialPositionInStream, KinesisUtils

from Config.AWSConfig import awsAccessKeyId, awsSecretKey
from Config.KinesisConfig import kinesisAppName, streamName, endpointUrl, regionName


class KinesisExtractor:
    def get_data_stream(self, ssc, window_size, window_slide):
        kinesis = KinesisUtils.createStream(ssc=ssc,
                                            kinesisAppName=kinesisAppName,
                                            streamName=streamName,
                                            endpointUrl=endpointUrl,
                                            regionName=regionName,
                                            initialPositionInStream=InitialPositionInStream.TRIM_HORIZON,
                                            checkpointInterval=10,
                                            awsAccessKeyId=awsAccessKeyId,
                                            awsSecretKey=awsSecretKey)

        return kinesis.window(window_size, window_slide)
