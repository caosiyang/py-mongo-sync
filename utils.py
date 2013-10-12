#!/usr/bin/env python

# filename: utils.py
# summary: utility functions
# author: caosiyang
# date: 2013/07/09

import sys
from subprocess import *


def run_command(cmd, log=False):
    """Execute a shell command, print stdout and return status code.
    """
    #print "[SHELL] %s" % cmd
    cmd_obj = Popen(cmd, stdout=PIPE, stderr=STDOUT, shell=True)
    output = ''
    while True:
        line = cmd_obj.stdout.readline()
        if not line:
            break
        output += line
        if log:
            print line[:-1]
    #cmd_obj.wait()
    cmd_obj.communicate()
    return True if cmd_obj.returncode == 0 else False, output


def gettime():
    """Get current time.
    """
    return 'now'


def log(log):
    """Print log information.
    """
    if log:
        print >> sys.stdout, '%s %s' % (log)
    else:
        print >> sys.stdout, '%s' % log 


def warning(log):
    """Print warning information.
    """
    print >> sys.stderr, '[WARNING] %s' % log if log else '[WARNING] '


def error(log):
    """Print error information.
    """
    print >> sys.stderr, '[ERROR] %s' % log if log else '[ERROR] '


def error_exit(log):
    """Print error information and terminate.
    """
    error(log)
    sys.exit(1)


def get_value_by_name(line, name, sep):
    """Get value of name in line.
    """
    if line and name and sep:
        tmp = [item.strip() for item in line.split(sep)]
        if len(tmp) == 2:
            return tmp[1].strip()
    return None
