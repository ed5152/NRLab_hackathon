#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun  9 17:06:41 2021

@author: ditter01
"""

import pandas as pd
import shutil

expidList_path = "/Volumes/nrlab/group/group_folders/GROUP/Hackathon/DNASeq_layouts_PASSED/"
files_filepath = '/Volumes/nrlab/group/group_folders/GROUP/Hackathon/LAYOUT/copy_LAYOUTS/haichao4/'

exp_list = pd.read_excel(expidList_path + '!EXPID_list.xlsx')
exp_list = exp_list.iloc[:,0]

for file in exp_list:
    shutil.copy(files_filepath + file + '_layout.xlsx',expidList_path + file + '_layout.xlsx', )
    print("Copied " + file + "across")