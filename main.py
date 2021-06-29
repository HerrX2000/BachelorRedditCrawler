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
import keyboard

class DebugLevel(Enum):
    ALWAYS = 0
    ERROR = 1
    WARN = 2
    INFO = 3
    DEBUG = 4

prw = praw.Reddit(user_agent='praw_overflow',client_id='SLcx5BHOfpE3bQ',client_secret='fowGwZ-GXfjG2TKpzp3u0gH8HeINgQ')

api = PushshiftAPI(prw)

DEBUG_LEVEL = DebugLevel.INFO
CHILL_TIME = 0.5


subreddits = ['berlin']
static_fieldnames = ['id','permalink','author', 'author_fullname', 'title', 'url', 'subreddit', 'stickied',  'created_utc', 'is_original_content','author_flair_text','is_video','locked','selftext','link_flair_richtext','domain','over_18']
dynamic_fieldnames = ['score','total_awards_received','upvote_ratio', 'num_comments']



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
    interval = 84*60*60

    posts = list()

    current_epoch = start_epoch
    warning_result_overflow = 0

    num_of_epochs = (end_epoch - start_epoch) // interval
    num_of_epochs_received = 0
    epoch_process_times = [10]

        
    suffix = 0
    path = ""
    while True:
        path = "posts_"+subreddit+"_"+str(suffix)+".csv"
        if Path(path).is_file() == False:
            debug("Writing r/"+subreddit+" to: "+path)
            break
        suffix += 1
        
    header = static_fieldnames+dynamic_fieldnames+['epoch','datetime']
    
    post_processed = 0
    post_process_times = [1]

    errors = list()

    with open(path, mode='w', newline='', encoding='utf-8') as post_file:
        post_writer = csv.writer(post_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        post_writer.writerow(header)
        prev_month = datetime.fromtimestamp(start_epoch).month
        prev_day = datetime.fromtimestamp(start_epoch).day


        while current_epoch < end_epoch:
            start_time = datetime.now()
            result = list(api.search_submissions(after=current_epoch, before=current_epoch+interval, subreddit=subreddit, filter=static_fieldnames, limit=200, sort_type='created_utc', sort='asc'))
            posts.extend(result)
            num_of_epochs_received += 1
            if len(result) >= 200:
                warning_result_overflow += 1
                errors.append(type('obj', (object,), {'type': 'EpochOverflow', 'epoch' : num_of_epochs_received}))
            current_epoch += interval
            debug("Received epoch "+datetime.fromtimestamp(current_epoch).strftime("%c")+" to "+datetime.fromtimestamp(current_epoch+interval).strftime("%c"), DEBUG_LEVEL.DEBUG)
            
             
            #time.sleep(CHILL_TIME)
            current_posts_processed = 0
            post_processed = 0

            this_post_process_times = [1]
            for post in result:
                try:
                    if keyboard.is_pressed('c'):
                        break
                    start_time = datetime.now()
                    post_as_csv = []
                    post.datetime = datetime.fromtimestamp(post.created_utc)
                    post.epoch = num_of_epochs_received
                    for fieldname in header:
                        if hasattr(post, fieldname):
                            post_as_csv.append(escape(getattr(post, fieldname)))
                        elif fieldname == 'author_fullname':
                            post_as_csv.append('NULL')
                        else:
                            debug("Warning: "+fieldname+' not found in: '+post.id, DebugLevel.DEBUG)
                            debug(post, DebugLevel.DEBUG)
                            errors.append(type('obj', (object,), {'type': 'PostAttrNotFound', 'post' : post}))
                            post_as_csv.append('NULL')
                    post_writer.writerow(post_as_csv)
                    this_post_process_times.append((datetime.now()-start_time).total_seconds())

                    post_processed += 1
                    eta_posts = ((len(result)-post_processed)*(sum(this_post_process_times) / len(this_post_process_times)))
                    eta = round((num_of_epochs-num_of_epochs_received)*(CHILL_TIME+sum(epoch_process_times) / len(epoch_process_times))+ eta_posts )
                    debug("\rReceived epoch "+str(num_of_epochs_received)+"/"+str(num_of_epochs)+"   \t|\tProcessed posts "+str(post_processed)+"/"+str(len(result))+"    \t|\t   Estimated time remaining: "+str(eta)+" sek   ", DEBUG_LEVEL.ALWAYS, True)        
                except OSError as err:
                    print("OS error: {0}".format(err))
                    errors.append(type('obj', (object,), {'type': 'UnknownError', 'epoch' : num_of_epochs_received}))
            post_process_times.append(this_post_process_times)
            #debug("Processed posts "+str(post_processed)+"/"+str(len(posts))+"\t|\t"+"Estimated time remaining: "+str(round((len(posts)-post_processed)*(sum(post_process_times) / len(post_process_times))))+" sek", DEBUG_LEVEL.ALWAYS, True)        

            epoch_process_times.append((datetime.now()-start_time).total_seconds())


            if keyboard.is_pressed('c'):
                print('Aborting...')
                debug('Latest post retrieved: '+str(datetime.fromtimestamp(max(post.created_utc for post in posts))), DebugLevel.ALWAYS)
                break

    debug('\nReceived for posts in r/'+subreddit+': '+ str(len(posts)))

    if(warning_result_overflow > 0):
        for error in errors:
            if(error.type == 'EpochOverflow'):
                debug('EpochOverflow Error in epoch: '+str(error.epoch), DebugLevel.ALWAYS)
            elif(error.type == 'PostAttrNotFound'):
                debug('PostAttrNotFound Error in post: '+str(error.post), DebugLevel.ALWAYS)
            elif(error.type == 'UnknownError'):
                debug('UnknownError Error in epoch: '+str(error.epoch), DebugLevel.ALWAYS)
            

    debug('Finished r/'+subreddit+'\n')
debug('Finished')