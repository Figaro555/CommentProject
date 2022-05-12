from Extractors.YouTube.AbstractDataExtractor import AbstractDataExtractor


class VideoDataExtractor(AbstractDataExtractor):

    def get_data(self, connector):

        v_response = None

        while v_response is None:
            try:
                request_v = connector.get_connection().videos().list(
                    chart='mostPopular',
                    part='statistics, snippet',
                    maxResults=5,
                    regionCode='UA'
                )
                v_response = request_v.execute()
                return v_response

            except Exception as _ex:
                v_response = None
                connector.index += 1
                if connector.index >= connector.api_arr_len:
                    raise Exception('Keys are expired')

        return v_response
