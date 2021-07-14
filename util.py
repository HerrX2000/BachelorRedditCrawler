from enum import Enum
import xml.dom.minidom as minidom
import sys
from datetime import datetime
from pathlib import Path

class DebugLevel(Enum):
    ALWAYS = 0
    ERROR = 1
    WARN = 2
    INFO = 3
    DEBUG = 4
    LOG_ONLY = 5

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
            txt = str(datetime.now()) + " | [" + debug_level_input.name + "]:  \t" + str(input) + "\n"
            myfile.write(txt)

def fix_len_int(int,len):
    return ("{:0"+str(len)+"d}").format(int)



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