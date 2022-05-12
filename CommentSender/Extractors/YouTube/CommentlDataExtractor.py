from Extractors.YouTube.AbstractDataExtractor import AbstractDataExtractor


class CommentDataExtractor(AbstractDataExtractor):

    def get_data(self, connector, video_id, pages):

        response = None
        while response is None:
            try:
                request = connector.get_connection().commentThreads().list(
                    part='snippet',
                    videoId=video_id,
                    order='time'
                )
                response = request.execute()
            except Exception as _ex:
                response = None
                connector.index += 1
                if connector.index >= connector.api_arr_len:
                    raise Exception('Keys are expired')

        result = response["items"]

        response_c = None

        for i in range(pages):
            while response_c is None:
                try:
                    request = connector.get_connection().commentThreads().list_next(
                        previous_request=request,
                        previous_response=response
                    )

                    response_c = request.execute()
                    result += response_c["items"]
                except Exception as _ex:
                    response_c = None
                    connector.index += 1
                    if connector.index >= connector.api_arr_len:
                        raise Exception('Keys are expired')

        return result
