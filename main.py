from psaw import PushshiftAPI
from datetime import datetime
import datetime as dt
import csv
import time
import praw

api = PushshiftAPI()

subreddits = ['berlin']
static_fieldnames = ['id','permalink','author', 'author_fullname', 'title', 'url', 'subreddit', 'upvote_ratio', 'num_comments', 'stickied',  'created_utc', 'is_original_content','author_flair_text','is_video','locked','selftext','link_flair_richtext','retrieved_on','domain','over_18']
dynamic_fieldnames = ['score','total_awards_received']

r = praw.Reddit(user_agent='praw_overflow',client_id='SLcx5BHOfpE3bQ',client_secret='fowGwZ-GXfjG2TKpzp3u0gH8HeINgQ')


def escape(input):
    if isinstance(input, str):
        input = input.replace('\n','\\n')
    return input

for subreddit in subreddits:
    start_epoch = int(dt.datetime(2021, 1, 1).timestamp())
    end_epoch = int(dt.datetime(2021, 1, 2).timestamp())
    interval = 2*60*60

    lst = list()

    current_epoch = start_epoch

    while current_epoch < end_epoch:
        result = list(api.search_submissions(after=current_epoch, before=current_epoch+interval, subreddit=subreddit, filter=static_fieldnames, limit=100, sort_type='created_utc', sort='asc'))
        lst.extend(result)
        if len(result) >= 100:
            print("WARNING!")
        current_epoch += interval
        time.sleep(1)

    with open('posts_january_'+subreddit+'.csv', mode='w', newline='', encoding='utf-8') as post_file:
        post_writer = csv.writer(post_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        post_writer.writerow(static_fieldnames+dynamic_fieldnames+['datetime'])

        for post in lst:
            post_as_csv = []
            for fieldname in static_fieldnames:
                if hasattr(post, fieldname):
                    post_as_csv.append(escape(getattr(post, fieldname)))
                elif fieldname == 'author_fullname':
                    post_as_csv.append('NULL')
                else:
                    print("Warning: "+fieldname+' not found in:')
                    print(post)
                    post_as_csv.append('NULL')
            post = r.submission(id=getattr(post, 'id'))
            for fieldname in dynamic_fieldnames:
                if hasattr(post, fieldname):
                    post_as_csv.append(escape(getattr(post, fieldname)))

            post_as_csv.append(datetime.fromtimestamp(post.created_utc))
            post_writer.writerow(post_as_csv)


    print ('Received posts in r/'+subreddit+': '+ str(len(lst)))