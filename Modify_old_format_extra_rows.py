#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 10 11:47:22 2021

@author: ditter01
"""

import pandas as pd
import os

file_Path = '/Volumes/nrlab/group/group_folders/GROUP/Hackathon/LAYOUT/copy_LAYOUTS/haichao4/'

files = os.listdir(file_Path)
files.sort()
# first file is hidden, second is directory!
files = files[2:]

updated_format = []

for file in files:
    f=pd.read_excel(file_Path + file , engine='openpyxl')
    
    print("reading in file " + file)
    
    if f.iloc[0,0] != "ClinicalSamples2 sampleName" : #and f.iloc[1,0] != "e.g.  GB3_CSF":
        strs = ["" for x in range(f.shape[1])]
        df2 = pd.DataFrame([strs,strs], columns = f.columns)
                
        f2 = pd.concat([df2, f])
        
        f2.iloc[0,0] = "ClinicalSamples2 sampleName"
        f2.iloc[1,0] = "e.g.  GB3_CSF"

        f2.to_excel(file_Path + file , index=False)
    
