#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun 11 14:33:13 2021

@author: ditter01
"""

import pandas as pd

inputfile_path = "/Volumes/nrlab/group/group_folders/GROUP/Hackathon/Chris_sampleName_replacement_01/Batch_2/"
input_info = pd.read_csv(inputfile_path + 'clinSample_HARD_fix_new_Sample_02.csv')

files = input_info.iloc[:,0]
error_list = input_info.iloc[:,1]
correct_list = input_info.iloc[:,2]
unique_files = files.unique()

file_Path = '/Volumes/nrlab/group/group_folders/GROUP/Hackathon/LAYOUT/copy_LAYOUTS/haichao4/'

error_message = []

for file in unique_files:
    f=pd.read_excel(file_Path + file + '_layout.xlsx', engine='openpyxl')
    print("reading in file " + file)
    
    unique_error_list = error_list[files==file].tolist()
    unique_correction_list = correct_list[files==file].tolist()
    
    for index, error in enumerate(unique_error_list):
        if f['sample_name'].str.contains(error).any() == True:
            f = f.replace(error, unique_correction_list[index])
            f.to_excel(file_Path + file + '_layout.xlsx', index=False)

        else: 
            print("Did not find sample name "+ error + " in file " + file )
            error_message.append("Did not find sample name "+ error + " in file " + file + "Correct sample_name is " +  unique_correction_list[index])
    
        
df = pd.DataFrame(error_message,columns=['Errors'])
df.to_excel(inputfile_path + 'Errors_from_Chris_HardFix.xlsx', index=False)  
        
    
