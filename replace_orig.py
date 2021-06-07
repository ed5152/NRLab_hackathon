#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun  4 16:43:52 2021

@author: ditter01
"""

import pandas as pd
import numpy as np
from math import floor
import os

inputFile_path = "/Volumes/nrlab/group/group_folders/GROUP/Hackathon/LAYOUT/copy_LAYOUTS/"
destination_filePath = "/Volumes/nrlab/group/group_folders/EXPERIMENTS/"

files = os.listdir(inputFile_path)

# First two files are test data, so:
files = files[2:]

for index, file in enumerate(files):
    file_numb = file[3:5]
    directory_numb = floor(np.int(file_numb)/5)*5
    
    os.rename(inputFile_path + file, destination_filePath + "EXP"+ np.str(directory_numb).zfill(2) + "/" + file[:7] + "/LAYOUT/" + file)
    