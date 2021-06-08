#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun  8 12:15:28 2021

@author: ditter01
"""

import pandas as pd
import numpy as np
import os

inputFile_path = "/Volumes/nrlab/group/group_folders/GROUP/Hackathon/LAYOUT/copy_LAYOUTS/haichao3/"

# EXP_ID
files = os.listdir(inputFile_path)
files.sort()

# Correction Type

for index, file in enumerate(files):

    # file_Path + "LAYOUT" + file
    f = pd.read_excel(inputFile_path + file)
    print("/n doing file" + file)



    # removing excess columns
    f.drop(f.iloc[:,:1], inplace = True, axis=1)

    f.to_excel(inputFile_path + file, index=False)

    #    if index<79:
    #   f = pd.read_excel(file_Path + "EXP"+ np.str(directory_numb) + "/" + file + "/LAYOUT/" + file + "_layout.xlsx") #file_Path + "LAYOUT" + file
    #  f = f.drop(['Unnamed: 0'], axis=1)
