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
z = r'F:\Environmental Baseline Data\Version 3\Data\PDF\A6F4Q4.pdf'
os.chdir(r'H:\GitHub\NGTL')


#xml tag <div> splitting point for pages
soup = BeautifulSoup(data['content'], 'lxml')
pages = soup.find_all('div', attrs={'class': 'page'})

#######################FUNCTIONS##################################################
def get_table_titles(page:int) -> list():#pd.DataFrame:
    tbl_names = list()
    text_per_page = [x.text for x in pages[page].find_all('p')]
    for x in text_per_page:
        if x.lower().startswith('table'):
            splt = re.split('\d+[\.:]?\s+',x) #one period or none after the last digit followed by one or more whitespace
            if len(splt) > 1:
                if splt[1].count('.') < 3 and splt[1][0].isupper() and 'table of content' not in x.lower():
                    tbl_names.append(x.replace('\n',''))
                
    return tbl_names
    #print(pages_text)
##################################################################################
title_dict = dict()

for ind, page in enumerate (pages):
    pg_num = ind
    chars = []
    try:
        tables = camelot.read_pdf(z, pages = str(pg_num+1), flag_size=True, copy_text=['v'], line_scale=40, f = 'excel')  #loop len(tables)
    except:
        continue
    title_lst = get_table_titles(pg_num)
    tb_num = tables.n
    #print(tb_num)
    #print(len(title_lst))
    print(title_lst)
    
    if tb_num == 0:
        print("No table on page "+ str(pg_num+1) + " is detected")
        continue
    
    elif tb_num == 1 and (tables[0].parsing_report)['whitespace'] > 80.0:
        print("Page {} contains an image".format((tables[0].parsing_report)['page'] ))  
        continue
    
    else:
        if len(title_lst) >= tb_num :
            for j in range(0,tb_num):
                df_tb = tables[j].df
                df_tb = df_tb.replace('/na', '_', regex = True)
                df_tb.columns = df_tb.iloc[0]
                df_tb = df_tb.iloc[1:]
                xl_name = title_lst[j]
                xl_name = xl_name.replace('/','_')
                xl_name = xl_name.replace(':','')
                
                #store page number, index of the table, and its name in a dictionary
                chars.append([j,df_tb,xl_name])
                
                xlsx_name = xl_name +'-'+str(pg_num+1)+'-'+str(j)+ '.xlsx'
                #xlsx_name = z.split('\\')[-1] + '-' + str(pg[i]+1) + str(j) + '.xlsx'
                df_tb.to_excel(xlsx_name, index = False, encoding='utf-8-sig')
            
            title_dict[pg_num] = chars
          
        elif len(title_lst) < tb_num :
            #first case: if the first table in the page is conitnuos of what was on the previous page
            df_tb = tables[0].df
            df_tb = df_tb.replace('/na', '_', regex = True)
            df_tb.columns = df_tb.iloc[0]
            df_tb = df_tb.iloc[1:]
            if (pg_num-1 in title_dict):
                #find the list of tables on the previous page
                lst_tbl = (title_dict.get(pg_num-1))[-1]
                lst_tbl_df = lst_tbl[1]
                #check if the columns of the last table on the previous page are the same as the table on this page
                if len((set(lst_tbl_df.columns)).difference(set(df_tb.columns))) == 0 or len(set(lst_tbl_df.columns))== len(set(df_tb.columns)):
                    xl_name = lst_tbl[2]
            
                    chars.append([0,df_tb,xl_name])
                    
                    xlsx_name = xl_name +'-'+str(pg_num+1)+'-'+str(j)+ '.xlsx'
                    #xlsx_name = z.split('\\')[-1] + '-' + str(pg[i]+1) + str(j) + '.xlsx'
                    df_tb.to_excel(xlsx_name, index = False, encoding='utf-8-sig')
                        
                else:
                    xl_name = title_lst[0]
                    xl_name = xl_name.replace('/','_')
                    xl_name = xl_name.replace(':','')
                       
                    chars.append([0,df_tb,xl_name])
                    xlsx_name = xl_name +'-'+str(pg_num+1)+'-'+str(j)+ '.xlsx'
                    #xlsx_name = z.split('\\')[-1] + '-' + str(pg[i]+1) + str(j) + '.xlsx'
                    df_tb.to_excel(xlsx_name, index = False, encoding='utf-8-sig')
                title_dict[pg_num] = chars
                
                if tb_num > 1:
                    for j in range(1,len(title_lst)):
                        df_tb = tables[j].df
                        df_tb = df_tb.replace('/na', '_', regex = True)
                        df_tb.columns = df_tb.iloc[0]
                        df_tb = df_tb.iloc[1:]
                        xl_name = title_lst[j]
                        xl_name = xl_name.replace('/','_')
                        xl_name = xl_name.replace(':','')
                        #store page number, index of the table, and its name in a dictionary
                        chars.append([j,df_tb,xl_name])
                        
                        xlsx_name = xl_name +'-'+str(pg_num+1)+'-'+str(j)+ '.xlsx'
                        #xlsx_name = z.split('\\')[-1] + '-' + str(pg[i]+1) + str(j) + '.xlsx'
                        df_tb.to_excel(xlsx_name, index = False, encoding='utf-8-sig')
                
                    for j in range(len(title_lst),tb_num):
                        df_tb = tables[j].df
                        df_tb = df_tb.replace('/na', '_', regex = True)
                        df_tb.columns = df_tb.iloc[0]
                        df_tb = df_tb.iloc[1:]
                        xl_name = z.replace('\\','_')
                        #store page number, index of the table, and its name in a dictionary
                        chars.append([j,df_tb,xl_name])                        
                        
                        xlsx_name = xl_name +'-'+str(pg_num+1)+'-'+str(j)+ '.xlsx'
                        #xlsx_name = z.split('\\')[-1] + '-' + str(pg[i]+1) + str(j) + '.xlsx'
                        df_tb.to_excel(xlsx_name, index = False, encoding='utf-8-sig')                    
                    title_dict[pg_num] = chars
                else:
                    title_dict[pg_num] = chars
            else:
                for j in range(0,len(title_lst)):
                    df_tb = tables[j].df
                    df_tb = df_tb.replace('/na', '_', regex = True)
                    df_tb.columns = df_tb.iloc[0]
                    df_tb = df_tb.iloc[1:]
                    xl_name = title_lst[j]
                    xl_name = xl_name.replace('/','_')
                    xl_name = xl_name.replace(':','')
                    #store page number, index of the table, and its name in a dictionary
                    chars.append([j,df_tb,xl_name])
                    
                    xlsx_name = xl_name +'-'+str(pg_num+1)+'-'+str(j)+ '.xlsx'
                    #xlsx_name = z.split('\\')[-1] + '-' + str(pg[i]+1) + str(j) + '.xlsx'
                    df_tb.to_excel(xlsx_name, index = False, encoding='utf-8-sig')
            
                for j in range(len(title_lst),tb_num):
                    df_tb = tables[j].df
                    df_tb = df_tb.replace('/na', '_', regex = True)
                    df_tb.columns = df_tb.iloc[0]
                    df_tb = df_tb.iloc[1:]
                    xl_name = z.replace('\\','_')
                    #store page number, index of the table, and its name in a dictionary
                    chars.append([j,df_tb,xl_name])                        
                    
                    xlsx_name = xl_name +'-'+str(pg_num+1)+'-'+str(j)+ '.xlsx'
                    #xlsx_name = z.split('\\')[-1] + '-' + str(pg[i]+1) + str(j) + '.xlsx'
                    df_tb.to_excel(xlsx_name, index = False, encoding='utf-8-sig')
                title_dict[pg_num] = chars
                    
    



##############PHASE 2##########################################################
            
files = ['A6F4Q3.pdf','A6F4Q4.pdf','A6F4Q5.pdf','A6F4Q6.pdf','A6F4Q7.pdf','A6F4Q8.pdf','A6F4Q9.pdf','A6F4R0.pdf','A6F4R1.pdf','A6F4R2.pdf']

tables = camelot.read_pdf(z, pages = '189', flag_size=True, copy_text=['v'], line_scale=40, f = 'excel')  #loop len(tables)
tables.n
rep = tables[0].parsing_report
rep['whitespace']
df_tb = tables[0].df
df_tb = df_tb.replace('/na', '_', regex = True)
df_tb.columns = df_tb.iloc[0]
df_tb = df_tb.iloc[1:]

for i in df_tb.columns:
    print(i)
tables.export('ffooo.csv')












