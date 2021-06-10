#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun  8 12:15:28 2021

@author: ditter01
"""

import pandas as pd
import numpy as np
from math import floor
import os
import glob

inputFile_path = "/Volumes/nrlab/group/group_folders/GROUP/Hackathon/LAYOUT/recovered_LAYOUTS/xls_files/"
files = os.listdir(inputFile_path)

torun_files = []
DNASeq_files = []
error_files = []

for file in files:
    if file[:1]!='~$' and file[-4:] =='xlsx':
        torun_files.append(file)

# File EXP1933_KH_layout.xlsx can't be opened for some reason,  'EXP2040_KH_layout.xlsx'
torun_files = torun_files[1:28,30:]

for file in torun_files:

    # read file
    f = pd.read_excel(inputFile_path + file)
    
    print("/n doing file" + file)    
        
    # Check is it DNASeq layout!
    if ('sample_name' in f.columns ) and ('sample_description' in f.columns) and ('SLX_ID' in f.columns) and ('barcode' in f.columns):
        DNASeq_files.append(file)
    
        # Name column 2 as further_comments
        if f.columns[2] == 'Unnamed: 2':
            f = f.rename(columns={'Unnamed: 2': 'further_comments'})
        elif f.columns[2] != 'further_comments':
            f.insert(2, "further_comments", np.empty([len(f), 1], dtype=str))
            print("inserting further comments column" + file)
    
        # remove sample type and case or control columns
        if ((f.columns[3] == 'sample_type') and (f.columns[4] == 'case_or_control')):
            f = f.drop(['sample_type', 'case_or_control'], axis=1)
            print("removing sample_type and case_or_control columns from" + file)
        else:
            print("no columns sample type or case or control found for " + file)
    
        # Fixing barcode\n       
        if f.columns[4] == 'barcode\n':
            f = f.rename(columns={'barcode\n': 'barcode'})
            #f=f.replace('barcode\n', 'barcode')
            print('replacing barcode\n with barcode')
        else:
            print("no barcode column found")
            # removing excess column
        
        f=f.drop(f.iloc[:,25:], inplace = True, axis=1)
        f.to_excel(inputFile_path + file , index=False)  

        
