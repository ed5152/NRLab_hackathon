#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun  4 16:43:52 2021

@author: ditter01
"""

import pandas as pd
import numpy as np
from math import floor

inputFile_path = "/Users/wang04"
input_list = pd.read_csv(inputFile_path + '/EXP_types.csv')

# EXP_ID
files = input_list.iloc[:, 1]

# Correction Type
file_type = input_list.iloc[:, 0]

file_Path = "/Volumes/files/research/nrlab/group/group_folders/GROUP/Hackathon/LAYOUT/copy_LAYOUTS/ "

for index, file in enumerate(files):
    file_numb = file[3:5]
    directory_numb = floor(np.int(file_numb)/5)*5

    f = pd.read_excel(file_Path + "EXP" + np.str(directory_numb) + "/" +
                      file + "/LAYOUT/" + file + "_layout.xlsx")  # file_Path + "LAYOUT" + file

    print("/n doing file" + file)

    if f.columns[2] != 'further_comments':
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

    # Name column 2 as further_comments
    if f.columns[2] == 'Unnamed_2':
        f = f.rename(column={'Unnamed: 2': 'further_comments'})

    f.to_excel(file_Path + "EXP" + np.str(directory_numb) + "/" +
               file + "/LAYOUT/" + file + "_layout.xlsx", index=False)

    #    if index<79:
    #   f = pd.read_excel(file_Path + "EXP"+ np.str(directory_numb) + "/" + file + "/LAYOUT/" + file + "_layout.xlsx") #file_Path + "LAYOUT" + file
    #  f = f.drop(['Unnamed: 0'], axis=1)
