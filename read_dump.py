import zstandard
import urllib.request
import util
import zreader
import json
from datetime import datetime
import os
import csv
from enum import Enum

class DebugLevel(Enum):
    ALWAYS = 0
    ERROR = 1
    WARN = 2
    INFO = 3
    DEBUG = 4
    LOG_ONLY = 5

subreddits = ['de']
fields = ['id', 'all_awardings', 'associated_award', 'author', 'author_created_utc', 'author_fullname', 'author_flair_text', 'awarders', 'body', 'collapsed_because_crowd_control', 'created_utc', 'gildings', 'is_submitter', 'link_id', 'locked', 'no_follow', 'parent_id', 'permalink', 'retrieved_on', 'score', 'send_replies', 'steward_reports', 'stickied', 'subreddit', 'subreddit_id', 'total_awards_received']
optional_fieldnames = ['author_fullname','steward_reports']





for i in range(2,13):
    path = util.get_next_file("./data/comments_subs_20_"+str(i), "csv")

    header = fields+optional_fieldnames+['datetime']
            

    with open(path, mode='w', newline='', encoding='utf-8') as comment_output:
        comment_writer = csv.writer(comment_output, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        comment_writer.writerow(header)
        file = util.download_file('https://files.pushshift.io/reddit/comments/RC_2020-'+util.fix_len_int(i,2)+'.zst')
        
        with open(file, 'r') as f:
            reader = zreader.Zreader(file, chunk_size=8192)

            # Read each line from the reader
            for line in reader.readlines():
                comment = json.loads(line)
                if comment['subreddit'] in subreddits:
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
