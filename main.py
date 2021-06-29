from psaw import PushshiftAPI
from datetime import datetime
from pathlib import Path
from enum import Enum
import datetime as dt
import csv
import praw
import sys  
import keyboard
import xml.dom.minidom as minidom


class DebugLevel(Enum):
    ALWAYS = 0
    ERROR = 1
    WARN = 2
    INFO = 3
    DEBUG = 4

DEBUG_LEVEL = DebugLevel.INFO
CHILL_TIME = 0.5

log_file = ""

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
    with open(log_file, "a") as myfile:
        txt = str(datetime.now()) + " | [" + debug_level_input.name + "]: " + input + "\n"
        myfile.write(txt)

def fix_len_int(int,len):
    return ("{:0"+str(len)+"d}").format(int)

config = {}


def configure():
    global config
    dic={}
    dom = minidom.parse("config.xml")
    root = dom.documentElement                        #take name1 as example
    config['user_agent'] =root.getElementsByTagName('user_agent')[0].firstChild.data
    config['client_id'] =root.getElementsByTagName('client_id')[0].firstChild.data
    config['client_secret'] =root.getElementsByTagName('client_secret')[0].firstChild.data
    config['start_date'] =root.getElementsByTagName('start_date')[0].firstChild.data
    config['end_date'] =root.getElementsByTagName('end_date')[0].firstChild.data    
    
def main():
    global config
    def abort():
        print('Aborting...')
        handle_errors(errors)
        debug('Latest post retrieved: '+str(datetime.fromtimestamp(max(post.created_utc for post in posts))), DebugLevel.ALWAYS)
        
    def handle_errors(errors):
        for error in errors:
            if(error.type == 'EpochOverflow'):
                debug('EpochOverflow Error in epoch: '+str(error.epoch), DebugLevel.ALWAYS)
            elif(error.type == 'PostAttrNotFound'):
                debug('PostAttrNotFound Error in post: '+str(error.fieldname), DebugLevel.ALWAYS)
                debug('\t'+str(error.post), DebugLevel.DEBUG)
            elif(error.type == 'UnknownError'):
                debug('UnknownError Error in epoch('+str(error.epoch)+'): '+str(error.error) , DebugLevel.ALWAYS)
                debug('\t'+str(error.post) , DebugLevel.DEBUG)

    prw = praw.Reddit(user_agent=config['user_agent'], client_id=config['client_id'], client_secret=config['client_secret'])

    api = PushshiftAPI(prw)

    subreddits = ['de']
    static_fieldnames = ['id','permalink','author', 'author_fullname', 'title', 'url', 'subreddit', 'stickied',  'created_utc', 'is_original_content','author_flair_text','is_video','locked','selftext','link_flair_richtext','domain','over_18']
    dynamic_fieldnames = ['score','total_awards_received','upvote_ratio', 'num_comments']

    start_epoch = int(dt.datetime(2020, 1, 21).timestamp())
    end_epoch = int(dt.datetime(2020, 12, 31).timestamp())
    interval = 16*60*60

    errors = list()

    for subreddit in subreddits:
        debug('Start receiving posts in r/'+subreddit)

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

        debug("Waiting for first server response...", DEBUG_LEVEL.ALWAYS)
                
        with open(path, mode='w', newline='', encoding='utf-8') as post_file:
            post_writer = csv.writer(post_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
            post_writer.writerow(header)


            while current_epoch < end_epoch:
                start_time_epoch = datetime.now()
                result = list(api.search_submissions(after=current_epoch, before=current_epoch+interval, subreddit=subreddit, filter=static_fieldnames, limit=200, sort_type='created_utc', sort='asc'))
                posts.extend(result)
                num_of_epochs_received += 1
                while len(result) >= 200:
                    interval -= 60*60
                    result = list(api.search_submissions(after=current_epoch, before=current_epoch+interval, subreddit=subreddit, filter=static_fieldnames, limit=200, sort_type='created_utc', sort='asc'))
                    errors.append(type('obj', (object,), {'type': 'EpochOverflow', 'epoch' : num_of_epochs_received}))
                    debug("EpochOverflow error occured, estimated time will be longer.")
                current_epoch += interval
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
                                errors.append(type('obj', (object,), {'type': 'PostAttrNotFound', 'post' : post, 'fieldname' : fieldname}))
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
                        debug("Received epoch "+fix_len_int(num_of_epochs_received, len(str(num_of_epochs)))+"/"+str(num_of_epochs)+"\t|\tProcessed posts "+fix_len_int(post_processed,3)+"/"+fix_len_int(len(result),3)+"\t|\tEstimated time remaining: "+str(round(eta/60,1))+" min", DEBUG_LEVEL.ALWAYS, True)        
                    except Exception as err:
                        debug(err)
                        errors.append(type('obj', (object,), {'type': 'UnknownError', 'epoch' : num_of_epochs_received, 'error' : err, 'post' : post}))
                if keyboard.is_pressed('c'):
                    abort()
                    break
                post_process_times.append(this_post_process_times)
                
                epoch_process_times.append((datetime.now()-start_time_epoch).total_seconds())
        debug('\nReceived for posts in r/'+subreddit+': '+ str(len(posts)))
        handle_errors(errors)
        debug('Finished r/'+subreddit+'\n')
    debug('Finished')

log_file = "log_"+str(datetime.now()).replace(" ", "_").replace(":", "_")+".txt"
open("log.txt", "w")
configure()
main()