from psaw import PushshiftAPI
from datetime import datetime
from pathlib import Path
from enum import Enum
import datetime as dt
import csv
import praw
import sys, traceback  
import keyboard
import util

class DebugLevel(Enum):
    ALWAYS = 0
    ERROR = 1
    WARN = 2
    INFO = 3
    DEBUG = 4
    LOG_ONLY = 5

DEBUG_LEVEL = DebugLevel.INFO
CHILL_TIME = 0.5
PS_LIMIT = 300
MIN_EXPECTED_POSTS = round( PS_LIMIT * 0.5 )

config = {}


def main():
    global config
    def abort():
        util.debug('\nAborting...')
        handle_errors(errors)
        util.debug('Latest post retrieved: '+str(datetime.fromtimestamp(max(post.created_utc for post in posts))), DebugLevel.ALWAYS)
        
    def handle_errors(errors):
        for error in errors:
            if(error.type == 'EpochOverflow'):
                util.debug('EpochOverflow Error in epoch: '+str(error.epoch), DebugLevel.ALWAYS)
            elif(error.type == 'PostAttrNotFound'):
                util.debug('PostAttrNotFound Error in post: '+str(error.fieldname), DebugLevel.ALWAYS)
                util.debug('\t'+str(error.post), DebugLevel.DEBUG)
            elif(error.type == 'UnknownError'):
                util.debug('UnknownError Error in epoch('+str(error.epoch)+'): '+str(error.error) , DebugLevel.ALWAYS)
                util.debug('\t'+str(error.post) , DebugLevel.DEBUG)

    prw = praw.Reddit(user_agent=config['user_agent'], client_id=config['client_id'], client_secret=config['client_secret'])

    api = PushshiftAPI(prw)

    subreddits = ['europe']
    static_fieldnames = ['id','permalink','author', 'author_fullname', 'title', 'url', 'subreddit', 'stickied',  'created_utc', 'is_original_content','author_flair_text','is_video','locked','selftext','link_flair_richtext','domain','over_18']
    dynamic_fieldnames = ['score','total_awards_received','upvote_ratio', 'num_comments']

    start_epoch = int(dt.datetime(2020, 8, 27).timestamp())
    end_epoch = int(dt.datetime(2020, 12, 31).timestamp())
    interval = 22*60*60

    errors = list()

    try:
        for subreddit in subreddits:
            util.debug('Start receiving posts in r/'+subreddit)

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
                    util.debug("Writing r/"+subreddit+" to: "+path)
                    break
                suffix += 1
                
            header = static_fieldnames+dynamic_fieldnames+['epoch','datetime']
            
            post_processed = 0
            post_process_times = list()

            util.debug("Waiting for first response from Pushshift...", DEBUG_LEVEL.ALWAYS)
                    
            with open(path, mode='w', newline='', encoding='utf-8') as post_file:
                post_writer = csv.writer(post_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
                post_writer.writerow(header)


                while current_epoch < end_epoch:
                    util.debug("Current interval: "+str(interval), DEBUG_LEVEL.LOG_ONLY)
                    start_time_epoch = datetime.now()
                    result = list(api.search_submissions(after=current_epoch, before=current_epoch+interval, subreddit=subreddit, filter=static_fieldnames, limit=PS_LIMIT, sort_type='created_utc', sort='asc'))
                    posts.extend(result)
                    retries = 0
                    prev_interval = interval
                    overflow = False
                    if len(result) >= PS_LIMIT or len(result) < MIN_EXPECTED_POSTS:
                        util.debug('\nProblems with Epoch, will dynamicly adjust interval size. Epoch start: '+str(current_epoch), DEBUG_LEVEL.WARN)
                    while len(result) >= PS_LIMIT or len(result) < MIN_EXPECTED_POSTS:
                        if( overflow == False and len(result) >= PS_LIMIT):
                            retries = 0
                        if(len(result) >= PS_LIMIT):
                            overflow = True
                        retries += 1
                        interval_change = ( 60*60 )*(retries**2)
                        if overflow:
                            interval_change = interval_change * -1
                        
                        new_interval = round(interval + interval_change)
                        
                        if overflow:
                            util.debug("EpochOverflow occured ("+str(len(result))+"/"+str(PS_LIMIT)+"). Changing interval time by "+str(round(interval_change/3600,1))+" hour and trying again. Previous interval: "+str(round(interval/3600,1))+" hrs. New interval: "+str(round(new_interval/3600,1))+" hrs. Try: "+str(retries), DEBUG_LEVEL.LOG_ONLY)
                            errors.append(type('obj', (object,), {'type': 'EpochUnderflow', 'epoch' : num_of_epochs_received+1, 'after' : current_epoch, 'before': current_epoch+interval}))
                        else:
                            util.debug("EpochUnderflow occured ("+str(len(result))+"/"+str(MIN_EXPECTED_POSTS)+"). Changing interval time by "+str(round(interval_change/3600,1))+" hour and trying again. Previous interval: "+str(round(interval/3600,1))+" hrs. New interval: "+str(round(new_interval/3600,1))+" hrs. Try: "+str(retries), DEBUG_LEVEL.LOG_ONLY)
                            errors.append(type('obj', (object,), {'type': 'EpochUnderflow', 'epoch' : num_of_epochs_received+1, 'after' : current_epoch, 'before': current_epoch+interval}))
                        
                        interval = new_interval
                        result = list(api.search_submissions(after=current_epoch, before=current_epoch+interval, subreddit=subreddit, filter=static_fieldnames, limit=PS_LIMIT, sort_type='created_utc', sort='asc'))
                        
                        num_of_epochs = num_of_epochs_received + ( (end_epoch - current_epoch) // interval )
                    
                    if(prev_interval != interval):
                        util.debug("Dynamicly adjusted interval time. Previous interval: "+str(round(prev_interval/3600,1))+" hrs. New interval: "+str(round(interval/3600,1))+" hrs.", DEBUG_LEVEL.INFO)

                    num_of_epochs_received += 1
                    current_epoch += interval
                    post_processed = 0

                    this_post_process_times = list()
                    
                    eta_epoch = -1
                    if len(epoch_process_times) != 0:
                        eta_epoch = round(num_of_epochs-num_of_epochs_received)*(sum(epoch_process_times) / len(epoch_process_times))

                    util.debug("Received epoch "+util.fix_len_int(num_of_epochs_received, len(str(num_of_epochs)))+"/"+str(num_of_epochs)+"\t|\tProcessed posts "+util.fix_len_int(post_processed,3)+"/"+util.fix_len_int(len(result),3)+"\t|\tEstimated time remaining: "+str(round(eta_epoch/60,1))+" min", DEBUG_LEVEL.ALWAYS, True)        

                    for post in result:
                        try:
                            start_time_post = datetime.now()
                            post_as_csv = []
                            post.datetime = datetime.fromtimestamp(post.created_utc)
                            post.epoch = num_of_epochs_received
                            for fieldname in header:
                                if hasattr(post, fieldname):
                                    post_as_csv.append(util.escape(getattr(post, fieldname)))
                                elif fieldname == 'author_fullname':
                                    post_as_csv.append('NULL')
                                else:
                                    util.debug("Warning: "+fieldname+' not found in: '+post.id, DebugLevel.DEBUG)
                                    util.debug(post, DebugLevel.DEBUG)
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
                                eta = round( eta_epoch + eta_posts )
                            util.debug("Received epoch "+util.fix_len_int(num_of_epochs_received, len(str(num_of_epochs)))+"/"+str(num_of_epochs)+"\t|\tProcessed posts "+util.fix_len_int(post_processed,3)+"/"+util.fix_len_int(len(result),3)+"\t|\tEstimated time remaining: "+str(round(eta/60,1))+" min", DEBUG_LEVEL.ALWAYS, True, False)        
                        except Exception as err:
                            util.debug(err, DEBUG_LEVEL.ERROR)
                            errors.append(type('obj', (object,), {'type': 'UnknownError', 'epoch' : num_of_epochs_received, 'error' : err, 'post' : post}))
                    
                    post_process_times.append(this_post_process_times)
                    
                    epoch_process_times.append((datetime.now()-start_time_epoch).total_seconds())
            util.debug('\nReceived posts in r/'+subreddit+': '+ str(len(posts)))
            handle_errors(errors)
            util.debug('Finished r/'+subreddit+'\n')
        util.debug('Finished')
    except KeyboardInterrupt:
        abort()
        print ("Saved progress.")
    except Exception:
        traceback.print_exc(file=sys.stdout)
        sys.exit(0)

Path("./log").mkdir(parents=True, exist_ok=True)
config = util.configure()
main()