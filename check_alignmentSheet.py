# -*- coding: utf-8 -*-
"""
Created on Fri Feb 14 09:52:23 2020

@author: yazdsous
"""

import pandas as pd

def is_alignmentsheet(file_name, pg_num):
    file_name = int(file_name)
    pg_num = str(pg_num+1)
    file_path = r'F:\Environmental Baseline Data\Version 4 - Final\Support files\Random Forest\Deliverable Results.csv'
    alignment_sheet = pd.read_csv(file_path, encoding = 'utf-8')
    #col_names = alignment_sheet.columns.values
    df = alignment_sheet
    list_of_aligsh = df[df.pdf_name == file_name]['ali_page_nos'].values[0].replace('[','').replace(']','').replace('\'','').replace(' ','').split(',')
    
    return (True if pg_num in list_of_aligsh else False)


        



