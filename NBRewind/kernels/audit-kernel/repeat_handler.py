#!/usr/bin/env python
import signal
import os
import psutil
import sys
import subprocess

def sigIntHandler(*_):
    print("Inside sigIntHandler")
    parent = psutil.Process(os.getpid())
    print("parent:", parent.exe())
    for child in parent.children(recursive=True):
        print("child:", child.exe())
        child.send_signal(signal.SIGKILL)

def sigTermHandler(*_):
    print("Inside sigTermHandler")
    parent = psutil.Process(os.getpid())
    print("parent:", parent.exe())
    for child in parent.children(recursive=True):
        print("child:", child.exe())
        child.send_signal(signal.SIGKILL)


print("Repeat Kernel")
sys.stdout = open('flinc.log', 'a')
sys.stderr = open('flinc.log', 'a')
signal.signal(signal.SIGINT, sigIntHandler)
signal.signal(signal.SIGTERM, sigTermHandler)
subprocess.run(["/bin/bash", "-c", f"AUDIT=false sciunit given {sys.argv[3]} repeat {sys.argv[5]} {sys.argv[6]} -f %"])

#subprocess.run(sys.argv[1:])