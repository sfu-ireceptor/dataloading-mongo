import pandas as pd
import json
import os
import tarfile
import sys
import re
import numpy as np
from os import listdir
from os.path import isfile, join
    
def main(mypath):
	files = [mypath + '/' + f for f in listdir(mypath) if (isfile(join(mypath, f)) and f.endswith(".txt")) ]

	l = []
	for filepath in files:
		#print(filepath)
		data = pd.read_table(filepath, skiprows=2)
		# print(data)
		l.append(data)

	t = l[0].loc[:, ['sample id']]
	t['total nb seq'] = l[0]['total results']

	query_type_list = ['equal', 'substring', 'regex', 'range', 'total']
	for query_type in query_type_list:
		if query_type + ' results' in l[0]:
			t[query_type + ' nb results'] = l[0][query_type + ' results']	
			for i,data in enumerate(l):
				t['run ' + str(i) + ' ' + query_type] = l[i][query_type + ' time']	

	print(t.to_csv(sep='\t'))
            
if __name__ == "__main__":
    mypath = sys.argv[1]
    main(mypath)
