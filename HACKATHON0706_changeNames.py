#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun  4 17:09:22 2021

@author: ditter01
"""

import pandas as pd

file_path = "smb://institute.cri.camres.org/files/research/nrlab/group/group_folders/GROUP/Hackathon/LAYOUT/"
input_info = pd.read_csv(file_path + 'ENTER FILE NAME')

files = input_info.iloc[:,0]


for file, index in enumerate(files):
    f=pd.read_csv(file_Path + file) #file_Path + "LAYOUT" + file
    f = f.replace(input_info[index,1],[index,2])
    f.to_csv(file_Path + 'file', index=False)
    
    
    
    
    
    
    
 ##   
#keep_col = ['day','month','lat','long']
#new_f = f[keep_col]
#new_f.to_csv("newFile.csv", index=False)