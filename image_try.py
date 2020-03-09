# -*- coding: utf-8 -*-
"""
Created on Wed Jan 29 15:50:55 2020

@author: yazdsous
"""

#!pip install PyMuPDF
import fitz
import os
import sys
import pandas as pd


#WHERE WE EXTRACT TABLE NAME FROM CSV
from bs4 import BeautifulSoup
from tika import parser
import os
import camelot
from fuzzywuzzy import fuzz
from datetime import datetime
import textwrap
import numpy as np
from pandas import Series


path = 'F:/Environmental Baseline Data/Version 4 - Final/Support files/Table titles raw data/final_table_titles2.csv'
df = pd.read_csv(path, usecols = ['page_number','final_table_title', 'Application title short', 'DataID_pdf', 'categories', 'Category'])
df = df[df['categories'] > 0] 
df = df[df['Category'] == 'Table']
df['final_table_title'] = df['final_table_title'].str.title()
df.head()

hearing = 'Application for Northwest Mainline Komie North Extension'
komie_north = df[df['Application title short'] == hearing].reset_index(drop = True)
komie_north.head()
files = list(komie_north['DataID_pdf'].unique())

tot = dict()


for file in files:
    file_path = 'F:/Environmental Baseline Data/Version 4 - Final/PDF/{}'.format(file)
    fname = file_path
    doc = fitz.open(file_path)
    img_ = dict()
    cnt = 0
    
    for page in doc:  # iterate through the pages
        cnt = cnt + 1
        d = page.getText("dict")
        blocks = d["blocks"]
        imgblocks = [b for b in blocks if b["type"] == 1]
        if len(imgblocks) != 0:
            img_[cnt] = imgblocks
    tot[file] = img_
###########################################################################################################       


#file = '729236.pdf'
#file = '729035.pdf'
file = '729032.pdf'
file_path = 'F:/Environmental Baseline Data/Version 4 - Final/PDF/{}'.format(file)
doc = fitz.open(file_path)
img_ = dict()
cnt = 0

for page in doc:  # iterate through the pages
    cnt = cnt + 1
    d = page.getText("dict")
    blocks = d["blocks"]
    imgblocks = [b for b in blocks if b["type"] == 1]
    if len(imgblocks) != 0:
        img_[cnt] = imgblocks



title_dict = dict()

data = parser.from_file(file_path,xmlContent=True)
#raw_xml = parser.from_file('A6T2V6.pdf', xmlContent=True)

#xml tag <div> splitting point for pages
soup = BeautifulSoup(data['content'], 'lxml')
pages = soup.find_all('div', attrs={'class': 'page'})
len(pages)
title_dict = dict()

lst_w = dict()
lst_h = dict()
for ind, page in enumerate (pages):
    pg_num = ind+1
    chars = []
    #camelot table objects for each page of the pdf
    try:
        tables = camelot.read_pdf(file_path, pages = str(pg_num), flag_size=True, copy_text=['v'],strip_text = '\n',line_scale=40, f = 'csv',flavour = 'stream')  #loop len(tables)
    except:
        continue
   
    tb_num = tables.n


    if (pg_num in img_) and tb_num == 0:
        lst_w[pg_num] = max(i['width'] for i in img_[pg_num])
        lst_h[pg_num] = max(i['height'] for i in img_[pg_num])
        print(pg_num,img_[pg_num][0]['width'],img_[pg_num][0]['height'])

        

w_val = np.array(list(lst_w.values())).mean()
h_val = np.array(list(lst_h.values())).mean()
for ind, page in enumerate (pages):
    pg_num = ind+1
    chars = []
    #camelot table objects for each page of the pdf
    try:
        tables = camelot.read_pdf(file_path, pages = str(pg_num), flag_size=True, copy_text=['v'],strip_text = '\n',line_scale=40, f = 'csv',flavour = 'stream')  #loop len(tables)
    except:
        continue
   
    tb_num = tables.n
    
    if (pg_num in img_) and tb_num != 0:
        if max(i['width'] for i in img_[pg_num]) > w_val:
            print("page {} contains a map".format(str(pg_num)))

        




def figure_specs(pages, file_path, img_):
    lst_w = dict()
    lst_h = dict()
    for ind, page in enumerate (pages):
        pg_num = ind+1
        #camelot table objects for each page of the pdf
        try:
            tables = camelot.read_pdf(file_path, pages = str(pg_num), flag_size=True, copy_text=['v'],strip_text = '\n',line_scale=40, f = 'csv',flavour = 'stream')  #loop len(tables)
        except:
            continue
        tb_num = tables.n
    
        if (pg_num in img_) and tb_num == 0:
            lst_w[pg_num] = max(i['width'] for i in img_[pg_num])
            lst_h[pg_num] = max(i['height'] for i in img_[pg_num])
            print(pg_num,img_[pg_num][0]['width'],img_[pg_num][0]['height'])
    w_val = np.array(list(lst_w.values())).mean()
    h_val = np.array(list(lst_h.values())).mean()
    return (w_val , h_val)


aa = figure_specs(pages, file_path,img_)

type(aa)
aa[0]


    
    
    