from datetime import datetime
from pathlib import Path
from enum import Enum
import datetime as dt
import csv
import praw
import sys, traceback  
import keyboard
import util
from pmaw import PushshiftAPI

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

    api = PushshiftAPI()

    subreddits = ['europe','de','berlin']
    static_fieldnames = ['all_awardings', 'associated_award', 'author', 'author_flair_text', 'awarders', 'body', 'collapsed_because_crowd_control', 'created_utc', 'gildings', 'id', 'is_submitter', 'link_id', 'locked', 'no_follow', 'parent_id', 'permalink', 'retrieved_on', 'score', 'send_replies', 'steward_reports', 'stickied', 'subreddit', 'subreddit_id', 'total_awards_received']
    dynamic_fieldnames = []

    

    start_epoch = int(dt.datetime(2020, 1, 1).timestamp())
    end_epoch = int(dt.datetime(2021, 1, 1).timestamp())
    interval = 12*60*60

    errors = list()

    try:
        for subreddit in subreddits:
            util.debug('Start receiving comments in r/'+subreddit)

            comments = list()
            
            suffix = 0
            path = ""
            while True:
                Path("./data").mkdir(parents=True, exist_ok=True)
                path = "./data/comments_"+subreddit+"_"+str(suffix)+".csv"
                if Path(path).is_file() == False:
                    util.debug("Writing r/"+subreddit+" to: "+path)
                    break
                suffix += 1
                
            header = static_fieldnames+dynamic_fieldnames+['datetime']
            
            post_processed = 0
            post_process_times = list()

            util.debug("Waiting for first response from Pushshift...", DEBUG_LEVEL.ALWAYS)
                    
            with open(path, mode='w', newline='', encoding='utf-8') as post_file:
                post_writer = csv.writer(post_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
                post_writer.writerow(header)


                result = list(api.search_comments(after=start_epoch, before=end_epoch, subreddit=subreddit))
                comments.extend(result)

                post_processed = 0

                this_post_process_times = list()

                #util.debug("Received epoch "+util.fix_len_int(num_of_epochs_received, len(str(num_of_epochs)))+"/"+str(num_of_epochs)+"\t|\tProcessed posts "+util.fix_len_int(post_processed,3)+"/"+util.fix_len_int(len(result),3)+"\t|\tEstimated time remaining: "+str(round(eta_epoch/60,1))+" min", DEBUG_LEVEL.ALWAYS, True)        


                for comment in comments:
                    try:
                        start_time_post = datetime.now()
                        post_as_csv = []
                        comment['datetime'] = datetime.fromtimestamp(comment['created_utc'])
                        for key in header:
                            if key in comment:
                                post_as_csv.append(util.escape(comment[key]))
                            elif key == 'author_fullname':
                                post_as_csv.append('NULL')
                            else:
                                util.debug("Warning: "+key+' not found in: '+comment['id'], DebugLevel.DEBUG)
                                util.debug(comment, DebugLevel.DEBUG)
                                errors.append(type('obj', (object,), {'type': 'PostAttrNotFound', 'post' : comment, 'fieldname' : key}))
                                post_as_csv.append('NULL')
                        post_writer.writerow(post_as_csv)
                        this_post_process_times.append((datetime.now()-start_time_post).total_seconds())

                        post_processed += 1
                        eta_posts = -1
                        if len(this_post_process_times) != 0:
                            eta_posts = ((len(result)-post_processed)*(sum(this_post_process_times) / len(this_post_process_times)))
                        eta = -1
                        #util.debug("Received epoch "+util.fix_len_int(num_of_epochs_received, len(str(num_of_epochs)))+"/"+str(num_of_epochs)+"\t|\tProcessed posts "+util.fix_len_int(post_processed,3)+"/"+util.fix_len_int(len(result),3)+"\t|\tEstimated time remaining: "+str(round(eta/60,1))+" min", DEBUG_LEVEL.ALWAYS, True, False)        
                    except Exception as err:
                        util.debug(err, DEBUG_LEVEL.ERROR)
                        errors.append(type('obj', (object,), {'type': 'UnknownError', 'error' : err, 'post' : comment}))
                
                post_process_times.append(this_post_process_times)
                
            util.debug('\nReceived for posts in r/'+subreddit+': '+ str(len(comment)))
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