# Parent Parser class of data file type specific AIRR data parsers
# Extracted common code patterns shared across various parsers.

from os.path import join
import pandas as pd

class Parser:

    def get_all_substrings(string):
        if type(string) == float:
            return
        else:
            length = len(string)
            for i in range(length):
                for j in range(i + 1, length + 1):
                    yield(string[i:j])

    def get_substring(string):
        strlist=[]
        for i in Parser.get_all_substrings(string):
            if len(i)>3:
                strlist.append(i)
        return strlist

    
    def __init__(self,context):
        self.context = context

    def getDataFolder(self):
        return self.context.library + "/"+self.context.type+"/"
    
    def getDataPath( self, fileName ):
        return join( self.getDataFolder(), fileName )

    scratchFolder = ""
    
    # We create a specific temporary 'scratch' folder for each sequence archive
    def setScratchFolder( self, fileName):
        folderName = fileName[:fileName.index('.')]
        self.scratchFolder = self.getDataFolder()+folderName + "/"

    def getScratchFolder(self):
        return self.scratchFolder
    
    def getScratchPath( self, fileName ):
        return join( self.getScratchFolder(), fileName )
    
    def readDf( self, fileName ):
        return pd.read_table( self.getScratchPath(fileName) )

    def readDfNoHeader( self, fileName ):
            return pd.read_table( self.getScratchPath(fileName), header=None )

    def process(self):
        return False

