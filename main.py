from pynput.keyboard import Key, Listener
from psaw import PushshiftAPI
from datetime import datetime
from pathlib import Path
from enum import Enum
import datetime as dt
import csv
import praw
import sys  
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


subreddits = ['de']
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

def on_press(key):
    if key == 'c':
        print('Aborting...')
        handle_errors(errors)
        debug('Latest post retrieved: '+str(datetime.fromtimestamp(max(post.created_utc for post in posts))), DebugLevel.ALWAYS)

def on_release(key):
    pass

def fix_len_int(int,len):
    return ("{:0"+str(len)+"d}").format(int)

def handle_errors(errors):
    if(warning_result_overflow > 0):
        for error in errors:
            if(error.type == 'EpochOverflow'):
                debug('EpochOverflow Error in epoch: '+str(error.epoch), DebugLevel.ALWAYS)
            elif(error.type == 'PostAttrNotFound'):
                debug('PostAttrNotFound Error in post: '+str(error.post), DebugLevel.ALWAYS)
            elif(error.type == 'UnknownError'):
                debug('UnknownError Error in epoch: '+str(error.epoch), DebugLevel.ALWAYS)

errors = list()
warning_result_overflow = 0

def main():
    for subreddit in subreddits:
        debug('Start receiving posts in r/'+subreddit)

        start_epoch = int(dt.datetime(2020, 1, 1).timestamp())
        end_epoch = int(dt.datetime(2020, 12, 31).timestamp())
        interval = 20*60*60

        posts = list()

        current_epoch = start_epoch

        num_of_epochs = (end_epoch - start_epoch) // interval
        num_of_epochs_received = 0
        epoch_process_times = []

            
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
        post_process_times = list()


        with open(path, mode='w', newline='', encoding='utf-8') as post_file:
            post_writer = csv.writer(post_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
            post_writer.writerow(header)
            prev_month = datetime.fromtimestamp(start_epoch).month
            prev_day = datetime.fromtimestamp(start_epoch).day


            while current_epoch < end_epoch:
                start_time_epoch = datetime.now()
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

                this_post_process_times = list()
                for post in result:
                    try:
                        if keyboard.is_pressed('c'):
                            break
                        start_time_post = datetime.now()
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
                        this_post_process_times.append((datetime.now()-start_time_post).total_seconds())

                        post_processed += 1
                        eta_posts = -1
                        if len(this_post_process_times) != 0:
                            eta_posts = ((len(result)-post_processed)*(sum(this_post_process_times) / len(this_post_process_times)))
                        eta = -1
                        if len(epoch_process_times) != 0:
                            eta = round((num_of_epochs-num_of_epochs_received)*(sum(epoch_process_times) / len(epoch_process_times))+ eta_posts )
                        debug("\rReceived epoch "+fix_len_int(num_of_epochs_received, len(str(num_of_epochs)))+"/"+str(num_of_epochs)+"\t|\tProcessed posts "+fix_len_int(post_processed,len(str(len(result))))+"/"+str(len(result))+"\t|\tEstimated time remaining: "+str(eta)+" sek", DEBUG_LEVEL.ALWAYS, True)        
                    except OSError as err:
                        print("OS error: {0}".format(err))
                        errors.append(type('obj', (object,), {'type': 'UnknownError', 'epoch' : num_of_epochs_received}))
                if keyboard.is_pressed('c'):
                    on_press('c')
                    break
                post_process_times.append(this_post_process_times)
                
                epoch_process_times.append((datetime.now()-start_time_epoch).total_seconds())
        debug('\nReceived for posts in r/'+subreddit+': '+ str(len(posts)))
        handle_errors(errors)
        debug('Finished r/'+subreddit+'\n')
    debug('Finished')

main()