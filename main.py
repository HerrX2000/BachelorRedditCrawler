from psaw import PushshiftAPI
from datetime import datetime
from pathlib import Path
from enum import Enum
import datetime as dt
import csv
import time
import praw
import sys
import time

class DebugLevel(Enum):
    ALWAYS = 0
    ERROR = 1
    WARN = 2
    INFO = 3
    DEBUG = 4

api = PushshiftAPI()

DEBUG_LEVEL = DebugLevel.INFO
CHILL_TIME = 0.5


subreddits = ['berlin']
static_fieldnames = ['id','permalink','author', 'author_fullname', 'title', 'url', 'subreddit', 'stickied',  'created_utc', 'is_original_content','author_flair_text','is_video','locked','selftext','link_flair_richtext','retrieved_on','domain','over_18']
dynamic_fieldnames = ['score','total_awards_received','upvote_ratio', 'num_comments']

r = praw.Reddit(user_agent='praw_overflow',client_id='SLcx5BHOfpE3bQ',client_secret='fowGwZ-GXfjG2TKpzp3u0gH8HeINgQ')


def escape(input):
    if isinstance(input, str):
        input = input.replace('\n','\\n')
    return input

def debug(input, debug_level_input = DebugLevel.ALWAYS, self_updating = False):
    if DEBUG_LEVEL.value >= debug_level_input.value:
        if(self_updating):
            sys.stdout.write("\r"+input+"       ")
            sys.stdout.flush()
        else:
            print(input)


for subreddit in subreddits:
    debug('Start receiving posts in r/'+subreddit)

    start_epoch = int(dt.datetime(2020, 1, 1).timestamp())
    end_epoch = int(dt.datetime(2020, 12, 31).timestamp())
    interval = 2*60*60

    posts = list()

    current_epoch = start_epoch
    warning_result_overflow = 0

    num_of_epochs = (end_epoch - start_epoch) // interval
    num_of_epochs_received = 0
    epoch_process_times = [1]

    while current_epoch < end_epoch:
        start_time = datetime.now()
        result = list(api.search_submissions(after=current_epoch, before=current_epoch+interval, subreddit=subreddit, filter=static_fieldnames, limit=100, sort_type='created_utc', sort='asc'))
        posts.extend(result)
        num_of_epochs_received += 1
        if len(result) >= 100:
            warning_result_overflow += 1
            debug("WARNING!")
        current_epoch += interval
        debug("Received epoch "+datetime.fromtimestamp(current_epoch).strftime("%c")+" to "+datetime.fromtimestamp(current_epoch+interval).strftime("%c"), DEBUG_LEVEL.DEBUG)
        
        debug("\rReceived epoch "+str(num_of_epochs_received)+"/"+str(num_of_epochs)+"\t|\tEstimated time remaining: "+str(round((num_of_epochs-num_of_epochs_received)*(CHILL_TIME+sum(epoch_process_times) / len(epoch_process_times))))+" sek", DEBUG_LEVEL.ALWAYS, True)        
        
        time.sleep(CHILL_TIME)
        epoch_process_times.append((datetime.now()-start_time).total_seconds())

    print ('\nReceived static data for posts in r/'+subreddit+': '+ str(len(posts)))

    if(warning_result_overflow > 0):
        debug("Possible epoch overflow: "+warning_result_overflow)

    header = static_fieldnames+dynamic_fieldnames+['datetime']
    
    suffix = 0
    path = ""
    while True:
        path = "posts_"+subreddit+"_"+str(suffix)+".csv"
        if Path(path).is_file() == False:
            debug("Writing r/"+subreddit+" to: "+path)
            break
        suffix += 1
            

    with open(path, mode='w', newline='', encoding='utf-8') as post_file:
        post_writer = csv.writer(post_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        post_writer.writerow(header)
        prev_month = datetime.fromtimestamp(start_epoch).month
        prev_day = datetime.fromtimestamp(start_epoch).day

        post_processed = 0
        post_process_times = [1]

        for post in posts:
            start_time = datetime.now()
            post_as_csv = []
            for fieldname in static_fieldnames:
                if hasattr(post, fieldname):
                    post_as_csv.append(escape(getattr(post, fieldname)))
                elif fieldname == 'author_fullname':
                    post_as_csv.append('NULL')
                else:
                    debug("Warning: "+fieldname+' not found in: '+post.id)
                    debug(post, DebugLevel.DEBUG)
                    post_as_csv.append('NULL')
            dynamic_post = r.submission(id=getattr(post, 'id'))
            for fieldname in dynamic_fieldnames:
                if hasattr(dynamic_post, fieldname):
                    post_as_csv.append(escape(getattr(dynamic_post, fieldname)))
                else:
                    debug("Warning: "+fieldname+' not found in:'+post.id)
                    debug(dynamic_post, DebugLevel.DEBUG)
                    post_as_csv.append('NULL')
            post_datetime = datetime.fromtimestamp(post.created_utc)
            post_as_csv.append(post_datetime)
            post_writer.writerow(post_as_csv)
            post_process_times.append((datetime.now()-start_time).total_seconds())

            post_processed += 1
            debug("Processed posts "+str(post_processed)+"/"+str(len(posts))+"\t|\t"+"Estimated time remaining: "+str(round((len(posts)-post_processed)*(sum(post_process_times) / len(post_process_times))))+" sek", DEBUG_LEVEL.ALWAYS, True)        

    debug('\nReceived dynamic data for posts in r/'+subreddit)
    debug('Finished r/'+subreddit+'\n')
debug('Finished')