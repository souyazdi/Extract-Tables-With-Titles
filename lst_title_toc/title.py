# -*- coding: utf-8 -*-
"""
Created on Wed Mar  4 23:51:34 2020

@author: yazdsous
"""
import sys
from tika import parser
from bs4 import BeautifulSoup
import re


file = 'A6T2W6.pdf'
file_path = 'F:/Environmental Baseline Data/Version 3/Data/PDF/{}'.format(file)
data = parser.from_file(file_path, xmlContent = True)

soup = BeautifulSoup(data['content'], 'lxml')
pages = soup.find_all('div', attrs={'class': 'page'})

length = len(pages)
tbl_names = list()
text_per_page = dict()
for i in range(0,length):
    text_per_page[i] = [x.text for x in pages[i].find_all('p')]
    
    
for key, value in text_per_page.items():
    #print(key,'->',value)
    for x in value:
        if x.lower().startswith('table'):
            splt = re.split('\d+[\.:]?\s+',x) #one period or none after the last digit followed by one or more whitespace
            if len(splt) > 1:
                if splt[1].count('.') < 3 and splt[1][0].isupper() and 'table of content' not in x.lower():
                    tbl_names.append(x.replace('\n',''))
                
