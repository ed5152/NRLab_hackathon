#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun  4 16:43:52 2021

@author: ditter01
"""

import pandas as pd
import numpy as np

file_path = 1#PATH TO FILES
input_list = pd.read_csv(file_path + 'ENTER FILE NAME')

# check which way round
files = input_list.iloc[:,0]
file_type = input_list.iloc[:,1]

for file, index in enumerate(files):
    f=pd.read_csv(file_Path + file) #file_Path + "LAYOUT" + file
    
    # dropping extra columns
    if file_type[index]!= "Type 34" || file_type[index]!= "Type 35":
        f = f.drop([sample_type], [case_or_control])
    
    # inserting column
    elif file_type[index]!= "Type 3" || file_type[index]!= "Type 4":
    f = f.insert(3, "further_comments", [])
    
    
    
    
    
    f.to_csv('file', index=False)