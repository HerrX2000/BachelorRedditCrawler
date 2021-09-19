import util
import zreader
import json
from datetime import datetime
import csv
from enum import Enum
import threading
import calendar
import time
import traceback
import sys

MAX_PROCESSES = 4

class DebugLevel(Enum):
    ALWAYS = 0
    ERROR = 1
    WARN = 2
    INFO = 3
    DEBUG = 4
    LOG_ONLY = 5

subreddits_cities = ['istanbul','paris','london','Madrid','Barcelona','rome','berlin']
subreddits_countries_native = ['de','france','es','Polska','Romania']
subreddits_countries_english = ['germany','spain','poland']
subreddits_countries_bi = ['russia','unitedkingdom','italy','ukraine','thenetherlands']
subreddits_countries = subreddits_countries_native + subreddits_countries_english + subreddits_countries_bi
subreddits_continent = ['europe']
subreddits = subreddits_cities + subreddits_countries + subreddits_continent
fields = ['id', 'all_awardings', 'associated_award', 'author', 'author_created_utc', 'author_flair_text', 'awarders', 'body', 'collapsed_because_crowd_control', 'created_utc', 'gildings', 'is_submitter', 'link_id', 'locked', 'no_follow', 'parent_id', 'permalink', 'retrieved_on', 'score', 'send_replies', 'stickied', 'subreddit', 'subreddit_id', 'total_awards_received']
optional_fieldnames = ['author_fullname','steward_reports']

downloading = False

completed = []

def start_process(range, i = 1):
    if(i in range):
        t = threading.Thread(target=processe_month, args = (range, i))
        t.daemon = True
        t.setName(calendar.month_name[i])
        util.debug("Starting process: "+t.name, DebugLevel.LOG_ONLY)
        while True:
            if(threading.active_count() <= MAX_PROCESSES):
                t.start()
                break
            else:
                time.sleep(10)



def processe_month(range, i = 1):
    download_file = util.download_file('https://files.pushshift.io/reddit/comments/RC_2020-'+util.fix_len_int(i,2)+'.zst', debug_level=DebugLevel.INFO, progress_bar=True)

    file = download_file.filepath

    start_process(range, i+1)
    
    with open(file, 'r') as f:
        try:     
            path = util.get_next_file("./data/comments_subs_20_"+str(i)+"_"+calendar.month_name[i], "csv")

            header = fields+optional_fieldnames+artifical_fields+['datetime']

            with open(path, mode='w', newline='', encoding='utf-8') as comment_output:
                comment_writer = csv.writer(comment_output, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
                comment_writer.writerow(header)
            
                reader = zreader.Zreader(file, chunk_size=8192)

                # Read each line from the reader
                line_num = 1
                for line in reader.readlines():
                    #if(line_num % 100000 == 0) and not downloading:
                    #        util.debug(threading.currentThread().name+": "+str(line_num)+" comments processed")
                    comment = json.loads(line)
                    comments_subreddit = comment['subreddit']
                    if comments_subreddit in subreddits:
                        comment_as_csv = []
                        comment['datetime'] = datetime.fromtimestamp(comment['created_utc'])
                        for key in header:
                            if key in comment:
                                comment_as_csv.append(util.escape(comment[key]))
                            elif key in optional_fieldnames:
                                comment_as_csv.append('NULL')
                            else:
                                util.debug("Warning: "+key+' not found in: '+comment['id'], DebugLevel.DEBUG)
                                util.debug(comment, DebugLevel.DEBUG)
                                comment_as_csv.append('NULL')
                        comment_writer.writerow(comment_as_csv)
                    line_num += 1
                util.debug("Total comments processed: "+str(line_num))
        except Exception as e:
            completed.append(threading.currentThread().name)
            util.debug("Unexpected error when processing: "+threading.currentThread().name, DebugLevel.ERROR)
            util.debug("Exception: "+str(e), DebugLevel.ERROR)
            util.debug("Stacktrace: "+str(traceback.format_exc()), DebugLevel.ERROR)
            return
    
    completed.append(threading.currentThread().name)
    util.debug("Finished: "+threading.currentThread().name)

range = range(8,13)

start_process(range, range[0])


while True:
    try:
        time.sleep(2)
        if(threading.active_count() <= 1 and len(completed) >= len(range)):
            util.debug("All threads completed")
            break
    except KeyboardInterrupt:
        exit()
    except:
        print("unknown error")