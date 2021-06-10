#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun  4 17:09:22 2021

@author: ditter01
"""

import pandas as pd

inputfile_path = "/Volumes/nrlab/group/group_folders/GROUP/Hackathon/Chris_sampleName_replacement_01/"
input_info = pd.read_csv(inputfile_path + 'clinSample_HARD_fix_new_Sample.csv')

files = input_info.iloc[:,0]
error_list = input_info.iloc[:,1]
correct_list = input_info.iloc[:,2]
unique_files = files.unique()

file_Path = '/Volumes/nrlab/group/group_folders/GROUP/Hackathon/LAYOUT/copy_LAYOUTS/haichao4/'

for file in unique_files:
    f=pd.read_excel(file_Path + file + '_layout.xlsx', engine='openpyxl')
    print("reading in file " + file)
    
    f = f.replace(error_list[files==file].tolist(), correct_list[files==file].tolist())
        
#    f = f.replace(error_list[files==file],correct_list[files==file])
    f.to_excel(file_Path + file + '_layout.xlsx', index=False)
    
    
    
    
    
    
    
 ##   
#keep_col = ['day','month','lat','long']
#new_f = f[keep_col]
#new_f.to_csv("newFile.csv", index=False)