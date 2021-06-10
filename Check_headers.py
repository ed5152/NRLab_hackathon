#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 10 10:49:22 2021

@author: ditter01
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun  4 17:09:22 2021

@author: ditter01
"""

import pandas as pd
import os

file_Path = '/Volumes/nrlab/group/group_folders/GROUP/Hackathon/LAYOUT/copy_LAYOUTS/haichao4/'

files = os.listdir(file_Path)
files.sort()
# first file is hidden, second is directory!
files = files[2:]

incorrect_format_list = []

for file in files:
    f=pd.read_excel(file_Path + file , engine='openpyxl')
    
    print("reading in file " + file)
    
    if f.iloc[0,0] != "ClinicalSamples2 sampleName" : #and f.iloc[1,0] != "e.g.  GB3_CSF":
        incorrect_format_list.append(file)

        
df = pd.DataFrame(incorrect_format_list,columns=['Incorrect_format'])
df.to_excel(file_Path + 'Incorrect_format_layouts.xlsx', index=False)
    
    
    
    
    
    
    
 ##   
#keep_col = ['day','month','lat','long']
#new_f = f[keep_col]
#new_f.to_csv("newFile.csv", index=False)