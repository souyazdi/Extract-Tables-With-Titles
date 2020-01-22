# -*- coding: utf-8 -*-
"""
Created on Thu Jan 16 11:13:58 2020

@author: yazdsous
"""

from bs4 import BeautifulSoup
from tika import parser
import os
import re
import camelot
from fuzzywuzzy import fuzz
from datetime import datetime
 


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

########################################################################
#start_time = datetime.now()
          
#files = ['3579528.pdf','3578647.pdf','3581069.pdf','3578648.pdf','3579739.pdf','3579849.pdf']

files = ['3578648.pdf']


#file_path = 'F:/Environmental Baseline Data/Version 4 - Final/PDF/{}'.format(files[0])
os.chdir(r'H:\GitHub\tmp\D')
#tables = camelot.read_pdf(file_path, pages = 'all', flag_size=True, copy_text=['v'], f = 'csv')  #loop len(tables)

    
title_dict = dict()
for file in files:
    file_path = 'F:/Environmental Baseline Data/Version 4 - Final/PDF/{}'.format(file)
    
    file_name = file_path.split('/')[-1].replace('.pdf','')
        
    data = parser.from_file(file_path,xmlContent=True)
    #raw_xml = parser.from_file('A6T2V6.pdf', xmlContent=True)

    #xml tag <div> splitting point for pages
    soup = BeautifulSoup(data['content'], 'lxml')
    pages = soup.find_all('div', attrs={'class': 'page'})

    title_dict = dict()
    start_time = datetime.now()
    for ind, page in enumerate (pages):
        pg_num = ind
        chars = []
        try:
            tables = camelot.read_pdf(file_path, pages = str(pg_num+1), flag_size=True, copy_text=['v'], f = 'csv')  #loop len(tables)
        except:
            continue
        title_lst = get_table_titles(pg_num)
        tb_num = tables.n
        
        print(title_lst)
        
        if tb_num == 0:
            print("No table on page "+ str(pg_num+1) + " is detected")
            continue
        
        elif tb_num == 1 and (tables[0].parsing_report)['whitespace'] > 69.0:

            print("Page {} contains an image".format((tables[0].parsing_report)['page'] ))  
            continue
        
        elif tb_num == 1:

            df_tb = tables[0].df
            df_tb = df_tb.replace('/na', '_', regex = True)
            df_tb.columns = df_tb.iloc[0]
            df_tb = df_tb.iloc[1:]
            
            if len(title_lst) == 0:

                if (pg_num-1 in title_dict):

                    #find the list of tables on the previous page
                    lst_tbl = (title_dict.get(pg_num-1))[-1]
                    lst_tbl_df = lst_tbl[1]
                    #check if the columns of the last table on the previous page are the same as the table on this page
                    
                    col_concat_curr = list(df_tb.columns.values)
                    ccc = ''.join(col_concat_curr)
                    col_join_curr = (ccc.replace(' ','')).replace('\n','')
                    col_concat_prev = list(lst_tbl_df.columns.values)
                    ccp = ''.join(col_concat_prev)
                    col_join_prev = (ccp.replace(' ','')).replace('\n','')  
                    ratio_similarity = fuzz.token_sort_ratio(col_join_curr, col_join_prev)
                
                   
                    if (len((set(lst_tbl_df.columns)).difference(set(df_tb.columns))) == 0) or (len(set(lst_tbl_df.columns))== len(set(df_tb.columns))) or (ratio_similarity >= 85):

                        xl_name = lst_tbl[2]
                                                
                        chars.append([0,df_tb,xl_name])
                                                
                        xlsx_name = file_name + '_' + xl_name +'_'+str(pg_num+1)+'_'+str(1)+ '.csv'
                       
                        print(xlsx_name)
                        #xlsx_name = z.split('\\')[-1] + '-' + str(pg[i]+1) + str(j) + '.xlsx'
                        df_tb.to_csv(xlsx_name, index = False, encoding='utf-8-sig')
                            
                    else:

                        xl_name = file_name
                        chars.append([0,df_tb,xl_name])
                        xlsx_name = file_name + '_' +str(pg_num+1)+'_'+str(1)+ '.csv'
                        df_tb.to_csv(xlsx_name, index = False, encoding='utf-8-sig')
                    title_dict[pg_num] = chars
                    
                else:
                    xl_name = file_name       
                    chars.append([0,df_tb,xl_name])
                    xlsx_name = file_name + '_' +str(pg_num+1)+'_'+str(1)+ '.csv'
                    df_tb.to_csv(xlsx_name, index = False, encoding='utf-8-sig')
                    title_dict[pg_num] = chars
            else:

                xl_name = title_lst[0]
                xl_name = xl_name.replace('/','_')
                xl_name = xl_name.replace(':','')
                #store page number, index of the table, and its name in a dictionary
                chars.append([0,df_tb,xl_name])
                xlsx_name = file_name + '_' + xl_name +'_'+str(pg_num+1)+'_'+str(1)+ '.csv'
                df_tb.to_csv(xlsx_name, index = False, encoding='utf-8-sig')
                title_dict[pg_num] = chars
        
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
                    
                    xlsx_name = file_name + '_' + xl_name +'_'+str(pg_num+1)+'_'+str(j+1)+ '.csv'
                    #xlsx_name = z.split('\\')[-1] + '-' + str(pg[i]+1) + str(j) + '.xlsx'
                    df_tb.to_csv(xlsx_name, index = False, encoding='utf-8-sig')
                
                title_dict[pg_num] = chars
              
            elif len(title_lst) < tb_num :
                try:
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
                        
                        col_concat_curr = list(df_tb.columns.values)
                        ccc = ''.join(col_concat_curr)
                        col_join_curr = (ccc.replace(' ','')).replace('\n','')
                        col_concat_prev = list(lst_tbl_df.columns.values)
                        ccp = ''.join(col_concat_prev)
                        col_join_prev = (ccp.replace(' ','')).replace('\n','')  
                        ratio_similarity = fuzz.token_sort_ratio(ccc, ccp)
                
                
                        if len((set(lst_tbl_df.columns)).difference(set(df_tb.columns))) == 0 or len(set(lst_tbl_df.columns))== len(set(df_tb.columns)) or ratio_similarity >= 85:
                            xl_name = lst_tbl[2]
                                                    
                            chars.append([0,df_tb,xl_name])
                                                    
                            xlsx_name = file_name + '_' + xl_name +'_'+str(pg_num+1)+'_'+str(1)+ '.csv'
                           
                            df_tb.to_csv(xlsx_name, index = False, encoding='utf-8-sig')
                                
                        else:
                            xl_name = file_name
                               
                            chars.append([0,df_tb,xl_name])
    
                            xlsx_name = file_name + '_' +str(pg_num+1)+'_'+str(1)+ '.csv'
    
                            df_tb.to_csv(xlsx_name, index = False, encoding='utf-8-sig')
                    else:
                        xl_name = file_name       
                        chars.append([0,df_tb,xl_name])
                        xlsx_name = file_name + '_' +str(pg_num+1)+'_'+str(1)+ '.csv'
                        df_tb.to_csv(xlsx_name, index = False, encoding='utf-8-sig')
                        title_dict[pg_num] = chars

                    indx = 0
                    for j in range(1,len(title_lst)+1):
                        df_tb = tables[j].df
                        df_tb = df_tb.replace('/na', '_', regex = True)
                        df_tb.columns = df_tb.iloc[0]
                        df_tb = df_tb.iloc[1:]
                        xl_name = title_lst[indx]
                        indx = indx + 1
                        xl_name = xl_name.replace('/','_')
                        xl_name = xl_name.replace(':','')
                        #store page number, index of the table, and its name in a dictionary
                        chars.append([j,df_tb,xl_name])
                        
                        xlsx_name = file_name + '_' + xl_name +'-'+str(pg_num+1)+'-'+str(j+1)+ '.csv'
                        #xlsx_name = z.split('\\')[-1] + '-' + str(pg[i]+1) + str(j) + '.xlsx'
                        df_tb.to_csv(xlsx_name, index = False, encoding='utf-8-sig')
                
                    for j in range(len(title_lst)+1,tb_num):
                        df_tb = tables[j].df
                        df_tb = df_tb.replace('/na', '_', regex = True)
                        df_tb.columns = df_tb.iloc[0]
                        df_tb = df_tb.iloc[1:]
                        xl_name = file_name
                        #store page number, index of the table, and its name in a dictionary
                        chars.append([j,df_tb,xl_name])                        
                        
                        xlsx_name = file_name + '_' + xl_name +'_'+str(pg_num+1)+'_'+str(j+1)+ '.csv'
                        #xlsx_name = z.split('\\')[-1] + '-' + str(pg[i]+1) + str(j) + '.xlsx'
                        df_tb.to_csv(xlsx_name, index = False, encoding='utf-8-sig')                    
                    title_dict[pg_num] = chars
                
                except:
                    print("Function failed on page {}".format(pg_num+1))
                    pass
                    
end_time = datetime.now()
print('Duration: {}'.format(end_time - start_time))                        
    
    
    







