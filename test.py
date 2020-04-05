# !usr/bin/env python
from mpi4py import MPI
import numpy as np
import json
from collections import Counter
import string
import subprocess


# function to handle json language code
def handleJSONLanuageCode(dataObj):
    lang_code = dataObj['doc']['metadata']['iso_language_code']
    if lang_code in languages_list:
        languages_list[lang_code] += 1
    else:
        languages_list[lang_code] = 1


# function to handle string language code
def handleStringLanguageCode(str):
    keyword = 'iso_language_code'
    start_index = str.find(keyword)
    lang_code_start_index = 0
    lang_code_end_index = 0
    flag = 0
    if start_index != -1:
        for i in range(start_index + len(keyword) + 1, len(str)):
            if flag == 0:
                if str[i] == '\"':
                    lang_code_start_index = i
                    flag += 1
            elif flag == 1:
                if str[i] == '\"':
                    lang_code_end_index = i
                    flag += 1
            elif flag == 2:
                break
    lang_code = str[lang_code_start_index + 1: lang_code_end_index]
    if lang_code in languages_list:
        languages_list[lang_code] += 1
    else:
        languages_list[lang_code] = 1


# function to handle json Hashtag code
def handleJSONHashtagCode(dataObj):
    hashtags = dataObj['doc']['entities']['hashtags']
    text_tags = dataObj['doc']['text']

    # takes all the hashtags in the 'hashtag' sub-section
    for tag in hashtags:
        hashtag = str(tag['text']).lower()
        if hashtag in hashtags_list:
            hashtags_list[hashtag] += 1
        else:
            hashtags_list[hashtag] = 1

    # takes all hashtags in the 'text' sub-section
    text_tags = [tag.strip("#").lower() for tag in text_tags.replace('#', ' #').split() if tag.startswith("#")]
    if text_tags:
        for tag in text_tags:
            # remove punctuation
            for punc in string.punctuation:
                if punc in tag:
                    tag = tag.replace(punc, '')

            if tag in hashtags_list:
                hashtags_list[tag] += 1
            else:
                hashtags_list[tag] = 1


# function to handle string Hashtag code
def handleStringHashtagCode(str):
    return None
    # print("search all str to find if there are hashtag code")


comm = MPI.COMM_WORLD
size = comm.size
rank = comm.rank

hashtags_list = {}              # hashtag dict
languages_list = {}             # languages dict
my_size = 0                     # size the current process should read
start_lines = 0                 # start point of current process
isSingleCoreMode = False        # using single core mode or not
file_name = 'bigTwitter.json'   # file name

# determine the mode
if size == 1:
    isSingleCoreMode = True
else:
    isSingleCoreMode = False

# calculate the total lines
if rank == 0:
    out = subprocess.run(['wc', '-l', file_name],
                         stdout=subprocess.PIPE).stdout.decode('utf-8')
    total_lines = int(out.strip().split()[0])
else:
    total_lines = 0

file = open(file_name)

if not isSingleCoreMode:
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

        start_lines = default_size * (rank - 1)    # assign different start lines to each process
else:
    my_size = total_lines

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
                else:
                    handleStringLanguageCode(dataStr)
        break       # does not allow each process go through the whole file
file.close()
comm.Barrier()

collect_result_language = comm.gather(languages_list, root=0)
collect_result_hashtags = comm.gather(hashtags_list, root=0)

if rank == 0:
    # language
    final_result_language = Counter({})
    for element in collect_result_language:
        final_result_language += Counter(element)

    # hashtag
    final_result_hashtags = Counter({})
    for hashtag_element in collect_result_hashtags:
        final_result_hashtags += Counter(hashtag_element)

    print(sorted(final_result_language.items(), key=lambda x: x[1], reverse=True)[0:10])
    print(sorted(final_result_hashtags.items(), key=lambda x: x[1], reverse=True)[0:10])

