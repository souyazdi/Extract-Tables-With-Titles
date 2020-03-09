# -*- coding: utf-8 -*-
"""
Created on Fri Mar  6 14:14:06 2020

@author: yazdsous
"""
#library
import pandas as pd
from bs4 import BeautifulSoup
from tika import parser
import os
import camelot
from fuzzywuzzy import fuzz
from datetime import datetime
import textwrap
#import fitz
import numpy as np
import time
import fitz


path = 'F:/Environmental Baseline Data/Version 4 - Final/Support files/Table titles raw data/final_table_titles5.csv'
df = pd.read_csv(path, usecols = ['page_number','final_table_title', 'Application title short', 'DataID_pdf','categories', 'Category'])
df = df[df['categories'] > 0] 
df = df[df['Category'] == 'Table']
df['final_table_title'] = df['final_table_title'].str.title()
df.head()


num_pdfs = list(df['DataID_pdf'].unique())
num_projects = list(df['Application title short'].unique())
num_pages = dict()

for prjct in num_projects:
    file = num_pdfs[num_projects.index(prjct)]
    file_path = 'F:/Environmental Baseline Data/Version 4 - Final/PDF/{}'.format(file)
    num_pages [prjct] = (fitz.open(file_path)).pageCount
    
    
    
    
    
    