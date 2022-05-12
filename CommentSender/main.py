from Connectors.YouTubeConnector import YouTubeConnector
from Extractors.YouTube.CommentlDataExtractor import CommentDataExtractor
from Extractors.YouTube.VideoDataExtractor import VideoDataExtractor
from Loaders.KinesisLoader import KinesisLoader


def lambda_handler(event, context):
    top_video_extr = VideoDataExtractor()
    comment_extr = CommentDataExtractor()
    connector = YouTubeConnector()

    kinesis_loader = KinesisLoader()

    top_videos: dict = top_video_extr.get_data(connector)
    pages = 1

    res = []

    for i in range(len(top_videos["items"])):
        res += comment_extr.get_data(YouTubeConnector(), top_videos["items"][i]["id"], pages)

    kinesis_loader.upload_to_stream(res, 'MCommentsStream')


