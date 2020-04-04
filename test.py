# !usr/bin/env python
from mpi4py import MPI
import numpy as np
import json
from collections import Counter


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


comm = MPI.COMM_WORLD
size = comm.size
rank = comm.rank

hashtags = {}           # hashtag dict
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
    start_lines = default_size * (rank - 1)    # assign different start lines to each process
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
                    # TODO write a new function to add up hashtags
                    # count language code
                    handleJSONLanuageCode(dataObj)
                else:
                    handleStringLanguageCode(dataStr)
        break       # does not allow each process go through the whole file
file.close()

# sort hashtag list based on appear times
# TODO sort hashtag list based on each value
# sort language list base on appear times
comm.Barrier()
collect_result = comm.gather(languages_list, root=0)
if rank == 0:
    final_result = Counter({})
    for element in collect_result:
        final_result += Counter(element)
    print(sorted(final_result.items(), key=lambda x: x[1], reverse=True)[0:10])

