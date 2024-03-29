#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun  4 17:09:22 2021

@author: ditter01
"""

import pandas as pd

inputfile_path = "/Volumes/nrlab/group/group_folders/GROUP/Hackathon/Chris_sampleName_replacement_01/"
input_info = pd.read_csv(inputfile_path + 'clinSample_EASY_FIX_replacement.csv')

files = input_info.iloc[:,0]
error_list = input_info.iloc[:,1]
correct_list = input_info.iloc[:,2]
unique_files = files.unique()

file_Path = '/Volumes/nrlab/group/group_folders/GROUP/Hackathon/LAYOUT/copy_LAYOUTS/haichao4/'

for file in unique_files:
    f=pd.read_excel(file_Path + file + '_layout.xlsx', engine='openpyxl')
    
    local_error_list = error_list[files==file]
    local_correction_list = correct_list[files==file]
    
    row_list = range(len(f))
    
    for index, sample_name in enumerate(local_error_list):
        
        
            
        f = f.replace(sample_name, local_correction_list[index])
        

    
#    f = f.replace(error_list[files==file],correct_list[files==file])
    f.to_excel(file_Path + 'file'+ '_layout.xlsx', index=False)
    
    
    
    
    
    
    
 ##   
#keep_col = ['day','month','lat','long']
#new_f = f[keep_col]
#new_f.to_csv("newFile.csv", index=False)