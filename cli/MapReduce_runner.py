import os
import lib.NameNode as nn
import json as j
import sys
from lib.dfs_tree import delim

'''
This module provides support to run map reduce jobs on YAH HDFS.
The command line arguments expected are:
    1.input_path : path to input file stored in DFS
    2.output_path : path to an empty directory in DFS where the output of reduce is stored
    3.config_path : path to config.json file which is used to start the DFS
    4.mapper_path : path to mapper file
    5.reducer_path : path to reducer file

The contents of the input file is stroed in temp.txt.
The output of the mapper file is stored in tempM.txt
The output of the reducer file is stored in tempR.txt
The contents of tempR.txt is stored in output_path/part-0000.txt and all temp files are deleted
'''

def json_to_dict(config_path):
    with open('config.json','r') as f:
        dicti = j.load(f)
        for key in dicti:
            try:
                dicti[key] = int(dicti[key])
            except:
                continue
    
    return dicti

# Checking the recieved arguments and initializing
#checking number of arguments
try:
    _,input_path,output_path,config_path,mapper_path,reducer_path = sys.argv
except Exception as E:
    print("Failed at expansion")
    print(E)
    quit()

# Loading DFS
try:
    config_dict = json_to_dict(config_path)
    nnNew = nn.NameNode(config_dict)
except Exception:
    print("Couldn't load dfs")
    quit()

# Checking input and output path
try:
    assert nnNew.ls(output_path) == []
except Exception as E:
    print("Invalid output path")
    print(nnNew.ls(output_path))
    quit()

try:
    nnNew.cat(input_path,'temp.txt')
except:
    print("Invalid input path")

# Running the map job:
try:
    os.system('type temp.txt | more |python %s | sort |python %s > tempR.txt'%(mapper_path,reducer_path))

except Exception as E:
    print("Failed at execution pipeline")
    print(E)
    quit()
    



# Storing output of reduce job and deleting temp files
try:
    nnNew.put('tempR.txt',output_path + delim + 'part-0000.txt')
    os.system('del temp.txt')
    os.system('del tempR.txt')
except Exception as E:
    print("Failed at storing")
    print(E)
