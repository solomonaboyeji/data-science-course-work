from datetime import datetime
import json
from pathlib import Path
# https://pypi.org/project/google-play-scraper/
from google_play_scraper import app
from google_play_scraper import Sort, reviews, reviews_all

comments = []
comments_ids = []

sorts = [Sort.MOST_RELEVANT, Sort.NEWEST]
filters = [ 1, 2, 3, 4, 5]
countries = [ "us", "uk", ]

continuation_token = None
for country in countries:
    for sort in sorts:
        for filter in filters:
            for page in range(0, 10):
                try:
                    result, continuation_token = reviews(
                        "com.amazon.mShop.android.shopping",
                        lang="en",  # defaults to 'en'
                        country=country,  # defaults to 'us'
                        sort=sort,
                        count=200,  # defaults to 100
                        filter_score_with=filter,
                    )  # type: ignore
                    for comment in result:
                        if comment.get("reviewId") not in comments_ids:
                            comment["at"] = str(comment.get("at"))
                            comment["country"] = country
                            comments.append(comment)
                            comments_ids.append(comment.get("reviewId"))
                    print(page, country, sort, filter, len(result))
                except Exception as e:
                    print(e)
Path(
    f"./result_{len(comments)}_reviews_{datetime.now().time().microsecond}.json"
).write_text(json.dumps(comments))
