from enum import Enum
import xml.dom.minidom as minidom
import sys
from datetime import datetime
from pathlib import Path
import requests
import os
from clint.textui import progress
import threading

class DebugLevel(Enum):
    ALWAYS = 0
    ERROR = 1
    WARN = 2
    INFO = 3
    DEBUG = 4
    LOG_ONLY = 5
    NONE = 999

log_file = ""

DEBUG_LEVEL = DebugLevel.INFO

def escape(input):
    if isinstance(input, str):
        input = input.replace('\n','\\n')
    return input

def debug(input, debug_level_input = DebugLevel.ALWAYS, self_updating = False, write_log = True):
    if DEBUG_LEVEL.value >= debug_level_input.value and debug_level_input != DEBUG_LEVEL.LOG_ONLY:
        if(self_updating):
            sys.stdout.write("\r"+input+"       ")
            sys.stdout.flush()
        else:
            print(input)
    if write_log:
        with open(log_file, "a") as myfile:
            txt = str(datetime.now()) + " | thread: " + threading.currentThread().name[:3] + " \t| [" + debug_level_input.name + "]:  \t" + str(input) + "\n"
            myfile.write(txt)

def fix_len_int(int,len):
    return ("{:0"+str(len)+"d}").format(int)

def get_next_file(filepath, file_extension):
    suffix = 0
    path = ""
    while True:
        Path("./data").mkdir(parents=True, exist_ok=True)
        path = filepath+"_"+str(suffix)+"."+file_extension
        if Path(path).is_file() == False:
            debug("Writing "+filepath+" to: "+path)
            return path
        suffix += 1

class DownloadedFile:
    def __init__(self, filepath: str, from_cache: bool):
        self.filepath = filepath
        self.from_cache = from_cache

def download_file(url, force = False, chunk_size = 16384, debug_level = DebugLevel.LOG_ONLY, progress_bar = True) -> DownloadedFile:
    local_filename = './downloads/'+url.split('/')[-1]
    # NOTE the stream=True parameter below
    


    request = requests.get(url, stream=True)
    resume_byte_position = None
    total_length = None
    resume = False

    total_length = int(request.headers.get('content-length'))
    if os.path.isfile(local_filename)  and not force:
        if os.stat(local_filename).st_size == total_length:
            debug(url+' already downloaded and using local file: '+local_filename, debug_level)
            return DownloadedFile(local_filename, True)
        elif os.stat(local_filename).st_size != 0:
            resume_header = {'Range': 'bytes=%d-' % os.stat(local_filename).st_size}
            request = requests.get(url, headers=resume_header, stream=True, allow_redirects=True)
            if request.status_code == 206:
                resume = True
                debug(url+' partialy downloaded and continuing now.')
            else:
                raise IOError('Requested partial download but server does not support it.') 

    with request as r:
        debug('Starting download of: '+url, debug_level)
        r.raise_for_status()
        with open(local_filename, 'ab' if resume else 'wb') as f:
            if(progress_bar):
                for chunk in progress.bar(r.iter_content(chunk_size=chunk_size), expected_size=(total_length/chunk_size) + 1): 
                    # If you have chunk encoded response uncomment if
                    # and set chunk_size parameter to None.
                    #if chunk: 
                    f.write(chunk)
            else:
                for chunk in r.iter_content(chunk_size=chunk_size): 
                    # If you have chunk encoded response uncomment if
                    # and set chunk_size parameter to None.
                    #if chunk: 
                    f.write(chunk)
            debug("Downloaded: "+local_filename, debug_level)

    return DownloadedFile(local_filename, False)


def configure():
    global log_file
    config = {}
    dom = minidom.parse("config.xml")
    root = dom.documentElement                        #take name1 as example
    config['user_agent'] =root.getElementsByTagName('user_agent')[0].firstChild.data
    config['client_id'] =root.getElementsByTagName('client_id')[0].firstChild.data
    config['client_secret'] =root.getElementsByTagName('client_secret')[0].firstChild.data
    config['start_date'] =root.getElementsByTagName('start_date')[0].firstChild.data
    config['end_date'] =root.getElementsByTagName('end_date')[0].firstChild.data

    
    Path("./log").mkdir(parents=True, exist_ok=True)
    log_file = "log/log_"+str(datetime.now()).replace(" ", "_").replace(":", "_")+".txt"
    return config

configure()