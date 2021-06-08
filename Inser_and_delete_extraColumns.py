#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun  8 12:15:28 2021

@author: ditter01
"""

import pandas as pd
import numpy as np
from math import floor

inputFile_path = "/Volumes/nrlab/group/group_folders/GROUP/Hackathon/LAYOUT"
input_list = pd.read_excel(inputFile_path + '/DNASeq_EXP_list_toTruncate.xlsx')

# EXP_ID
files = input_list.iloc[:,0]

# Correction Type

file_Path = "/Volumes/nrlab/group/group_folders/GROUP/Hackathon/LAYOUT/copy_LAYOUTS/"

for index, file in enumerate(files):

    # file_Path + "LAYOUT" + file
    f = pd.read_excel(file_Path + file + "_layout.xlsx")

    print("/n doing file" + file)

        # Name column 2 as further_comments
    if f.columns[2] == 'Unnamed: 2':
        f = f.rename(columns={'Unnamed: 2': 'further_comments'})
    elif f.columns[2] != 'further_comments':
        f.insert(2, "further_comments", np.empty([len(f), 1], dtype=str))
        print("inserting further comments column" + file)

    if ((f.columns[3] == 'sample_type') and (f.columns[4] == 'case_or_control')):
        f = f.drop(['sample_type', 'case_or_control'], axis=1)
        print("removing sample_type and case_or_control columns from" + file)
    else:
        print("no columns sample type or case or control found for " + file)

    if f.columns[4] == 'barcode\n':
        f = f.rename(columns={'barcode\n': 'barcode'})
        #f=f.replace('barcode\n', 'barcode')
        print('replacing barcode\n with barcode')
    else:
        print("no barcode column found")

    # removing excess columns
    f.drop(f.iloc[:,25:], inplace = True, axis=1)

    f.to_excel(file_Path + file + "_layout.xlsx", index=False)

    #    if index<79:
    #   f = pd.read_excel(file_Path + "EXP"+ np.str(directory_numb) + "/" + file + "/LAYOUT/" + file + "_layout.xlsx") #file_Path + "LAYOUT" + file
    #  f = f.drop(['Unnamed: 0'], axis=1)
