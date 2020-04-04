# !usr/bin/env python
from mpi4py import MPI
import numpy as np
import json
from collections import Counter

comm = MPI.COMM_WORLD
size = comm.size
rank = comm.rank

hashtags = {}           # hashtag dict
languages_list = {}     # languages dict
my_size = 0             # size the current process should read
start_lines = 0         # start point of current process

file = open("tinyTwitter.json")
# get total rows in the file (not include 1st row)
total_lines = int(file.readline().split(",")[0].split(':')[1])
# default size a process should read
default_size = np.ceil(total_lines / size)
# the size that last process should read
end_size = total_lines - (default_size * (size - 1))
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
            if dataStr != '':
                dataObj = json.loads(dataStr[: -2])     # remove \n and ,
                # count hashtag contents
                # TODO write a new function to add up hashtags
                # count language code
                lang_code = dataObj['doc']['metadata']['iso_language_code']
                if lang_code in languages_list:
                    languages_list[lang_code] += 1
                else:
                    languages_list[lang_code] = 1
        break                   # does not allow each process go through the whole file
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
    # print(final_result)
    print(sorted(final_result.items(), key=lambda x: x[1], reverse=True)[0:9])