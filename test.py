# !usr/bin/env python

# on local dir, run cmd: mpirun -np 4 python3 test.py

from mpi4py import MPI
import numpy as np
import json
from collections import Counter
import string

# function to handle json language code
def handleJSONLanuageCode(dataObj):
    lang_code = dataObj['doc']['metadata']['iso_language_code']
    if lang_code in languages_list:
        languages_list[lang_code] += 1
    else:
        languages_list[lang_code] = 1

# function to handle string language code
def handleStringLanguageCode(str):
    return None
    # print("search all str to find if there are language code")


# function to handle json Hashtag code
def handleJSONHashtagCode(dataObj):
    hashtag_code = dataObj['doc']['entities']['hashtags']
    text_tags = dataObj['doc']['text']

    #takes all the hashtags in the 'hashtag' sub-section
    if hashtag_code:
        hashtag_code = str(hashtag_code[0]['text']).lower()
        if hashtag_code in hashtags_List:
            hashtags_List[hashtag_code] += 1
        else:
            hashtags_List[hashtag_code] = 1

    #takes all hashtags in the 'text' sub-section
    text_tags = [tag.strip("#").lower() for tag in text_tags.replace('#',' #').split() if tag.startswith("#")]
    if text_tags:
        for tag in text_tags:
            #remove punctuation
            for punc in string.punctuation:
                if punc in tag:
                    tag = tag.replace(punc,'')

            if tag in hashtags_List:
                hashtags_List[tag] += 1
            else:
                hashtags_List[tag] = 1
    
# function to handle string Hashtag code
def handleStringHashtagCode(str):
    return None
    # print("search all str to find if there are hashtag code")


comm = MPI.COMM_WORLD
size = comm.size
rank = comm.rank

hashtags_List = {}           # hashtag dict
languages_list = {}     # languages dict
my_size = 0             # size the current process should read
start_lines = 0         # start point of current process

# amode = MPI.MODE_WRONLY|MPI.MODE_CREATE
# f = MPI.File.Open(comm, "tinyTwitter.json", amode)
file = open("tinyTwitter.json")
# get total rows in the file (not include 1st row)
# total_lines = int(file.readline().split(",")[0].split(':')[1])
# calculate the total lines
if rank == 0:
    total_lines = 0
    for index, line in enumerate(file):
        total_lines = index + 1
else:
    total_lines = 0
# when broadcast rank 0 does not work on the task
total_lines = comm.bcast(total_lines, root=0)
# default size a process should read
default_size = np.ceil(total_lines / (size - 1))
# the size that last process should read
end_size = total_lines - (default_size * (size - 2))
# assign the size that current process should read
for i in range(0, size):
    if rank == size - 1:
        my_size = end_size
    if rank == i:
        my_size = default_size
    start_lines = default_size * rank    # assign different start lines to each process
# read the file from start point

for index, line in enumerate(file):
    if index == int(start_lines):
        for j in range(0, int(my_size)):
            dataStr = file.readline()
            validJSON = True
            if dataStr != '':
                dataObj = {}
                try:
                    dataObj = json.loads(dataStr[: -2])     # remove \n and ,
                except ValueError as e:
                    validJSON = False
                if validJSON:
                    # count hashtag contents
                    handleJSONHashtagCode(dataObj)
                    # count language code
                    handleJSONLanuageCode(dataObj)
                # else:
                #     handleStringHashtagCode(dataStr)
                #     handleStringLanguageCode(dataStr)
        break       # does not allow each process go through the whole file

file.close()

# sort hashtag list based on appear times
# TODO sort hashtag list based on each value
# sort language list base on appear times
comm.Barrier()
collect_result_language = comm.gather(languages_list, root=0)
collect_result_hashtags = comm.gather(hashtags_List, root=0)

if rank == 0:
    # language
    final_result_language = Counter({})
    for element in collect_result_language:
        final_result_language += Counter(element)
    
    #hashtag
    final_result_hashtags = Counter({})
    for hashtag_element in collect_result_hashtags:
        final_result_hashtags += Counter(hashtag_element)

    # print(final_result)
    print(sorted(final_result_language.items(), key=lambda x: x[1], reverse=True)[0:10])
    print(sorted(final_result_hashtags.items(), key=lambda x: x[1], reverse=True)[0:10])


