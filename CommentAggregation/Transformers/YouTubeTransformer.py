class YouTubeTransformer:
    def like_transformer(self, comment):
        try:
            return (comment["id"], comment["snippet"]["videoId"],
                    comment["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                    comment["snippet"]["topLevelComment"]["snippet"]["publishedAt"],
                    comment["snippet"]["topLevelComment"]["snippet"]["likeCount"])
        except Exception as _ex:
            print(_ex)
            return (" ", " ", " ", " ", 0)

    def length_transformer(self, comment):
        try:
            return (comment["id"], comment["snippet"]["videoId"],
                    comment["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                    comment["snippet"]["topLevelComment"]["snippet"]["publishedAt"],
                    len(comment["snippet"]["topLevelComment"]["snippet"]["textDisplay"]))
        except Exception as _ex:
            print(_ex)
            return (" ", " ", " ", " ", 0)

    def reply_transformer(self, comment):
        try:
            return (comment["id"], comment["snippet"]["videoId"],
                    comment["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                    comment["snippet"]["topLevelComment"]["snippet"]["publishedAt"],
                    comment["snippet"]["totalReplyCount"])
        except Exception as _ex:
            print(_ex)
            return (" ", " ", " ", " ", 0)
