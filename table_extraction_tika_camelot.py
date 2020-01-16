# -*- coding: utf-8 -*-
"""
Created on Thu Jan 16 11:13:58 2020

@author: yazdsous
"""

from io import StringIO
from bs4 import BeautifulSoup
from tika import parser
import pandas as pd
import os
import re
import pandas as pd
import camelot
from collections import deque
import copy


os.chdir(r'F:\Environmental Baseline Data\Version 3\Data\PDF')
#A6A2D6.pdf
############A6T2V6.pdf
#A6A2D2.pdf
data = parser.from_file('A6F4Q4.pdf',xmlContent=True)
#raw_xml = parser.from_file('A6T2V6.pdf', xmlContent=True)

#xml tag <div> splitting point for pages
soup = BeautifulSoup(data['content'], 'lxml')
pages = soup.find_all('div', attrs={'class': 'page'})


def get_table_titles(page:int) -> list():#pd.DataFrame:
    tbl_names = list()
    text_per_page = [x.text for x in pages[page].find_all('p')]
    for x in text_per_page:
        if x.lower().startswith('table'):
            splt = re.split('\d+[\.?:?]\s+',x) #one period or none after the last digit followed by one or more whitespace
            if len(splt) > 1:
                if splt[1].count('.') < 3 and splt[1][0].isupper() and 'table of content' not in x.lower():
                    tbl_names.append(x.replace('\n',''))
                
    return tbl_names
    #print(pages_text)
z = r'F:\Environmental Baseline Data\Version 3\Data\PDF\A6F4Q4.pdf'
os.chdir(r'H:\GitHub\NGTL')

for ind, page in enumerate (pages):
    pg_num = ind
    try:
        tables = camelot.read_pdf(z, pages = str(pg_num+1), flag_size=True, copy_text=['v'], line_scale=40, f = 'excel')  #loop len(tables)
    except:
        continue
    title_lst = get_table_titles(pg_num)
    tb_num = tables.n
    print(tb_num)
    print(len(title_lst))
    print(title_lst)
    if tb_num == 0:
        print("No table on page "+ str(pg_num) + " is detected")
    else:
        if len(title_lst) == tb_num :
            for j in range(0,tb_num):
                df_tb = tables[j].df
                df_tb = df_tb.replace('/na', '_', regex = True)
                df_tb.columns = df_tb.iloc[0]
                df_tb = df_tb.iloc[1:]
                xl_name = title_lst[j]
                xl_name = xl_name.replace('/','_')
                xl_name = xl_name.replace(':','')
        
                xlsx_name = xl_name +'-'+str(pg_num+1)+'-'+str(j)+ '.xlsx'
                #xlsx_name = z.split('\\')[-1] + '-' + str(pg[i]+1) + str(j) + '.xlsx'
                df_tb.to_excel(xlsx_name, index = False, encoding='utf-8-sig')
        else:
            print("Number of titles and tables do not match")


##############PHASE 2##########################################################
            
files = ['A6F4Q3.pdf','A6F4Q4.pdf','A6F4Q5.pdf','A6F4Q6.pdf','A6F4Q7.pdf','A6F4Q8.pdf','A6F4Q9.pdf','A6F4R0.pdf','A6F4R1.pdf','A6F4R2.pdf']
















