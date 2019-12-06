# Parent Parser class of data file type specific AIRR data parsers
# Extracted common code patterns shared across various parsers.

from os.path import join
from datetime import datetime
from datetime import timezone
import re
import os
import pandas as pd


class Parser:

    # Class constructor
    def __init__(self, context):
        # Keep track of the global context for this parser.
        # The context should probably be refactored...
        self.context = context
        # A helper variable that parser's can use if they need to create a scratch
        # folder for storing temporary data in it. The folder name is guaranteed to
        # be unique to the process being run, as it uses the process id in the folder
        # name.
        self.scratchFolder = ""

    @staticmethod
    def getDateTimeNowUTC():
        return datetime.now(timezone.utc).strftime("%a %b %d %Y %H:%M:%S %Z")

    def verbose(self):
        return self.context.verbose

    def getDataFolder(self, fileWithPath):
        # Data folder is based on the path to the file.
        data_dir = os.path.dirname(fileWithPath)
        # If there is no path, then the data directory is the current directory.
        if (not os.path.isdir(data_dir)):
            data_dir = "."
        return data_dir


    # We create a specific temporary 'scratch' folder for each sequence archive
    def setScratchFolder(self, fileWithPath, fileName):
        folderName = fileName[:fileName.index('.')]
        self.scratchFolder = self.getDataFolder(fileWithPath) + "/tmp_" + str(os.getpid()) + "_" + folderName + "/"

    def getScratchFolder(self):
        return self.scratchFolder
