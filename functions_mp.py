
from bs4 import BeautifulSoup
import lxml
from tika import parser
import os
import sys
import camelot
from fuzzywuzzy import fuzz
from datetime import datetime
import textwrap
import pandas as pd
sys.path.insert(0, 'H:/GitHub/Extract-Tables-With-Titles')
import check_alignmentSheet as ca
import numpy as np
from PyPDF2 import PdfFileReader
import json
#######################FUNCTIONS##################################################
def get_table_titles(page:int,file,df:pd.DataFrame) -> list():#pd.DataFrame:
#    tbl_names_trunc = list()
    tbl_names = list(df[(df['page_number']== page) & (df['DataID_pdf']== file)]['final_table_title'])
#    for tbl in tbl_names:
#        if len(tbl) > 218:
#            tbl_names_trunc.append(textwrap.shorten(tbl,width = 50))
#        else:
#            tbl_names_trunc.append(tbl)      
#    return tbl_names_trunc
    return tbl_names
#********************************************************************************#
def replace_chars(text:str) -> str:
    chars_0 = ['\n',':']
    chars_1 = ['/','\\']
    for c in chars_0:
        text = text.replace(c, ' ')
    for cc in chars_1:
        text = text.replace(cc,'_')
    return text
#********************************************************************************#
def replace_chars_strings(lst:list) -> list:
    new_lst = []
    for itm in lst:
        new_lst.append(replace_chars(itm))       
    return new_lst

#*******************************************************************************#
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
            #print(pg_num,img_[pg_num][0]['width'],img_[pg_num][0]['height'])
    w_val = np.array(list(lst_w.values())).mean()
    h_val = np.array(list(lst_h.values())).mean()
    return (w_val , h_val)
#*******************************************************************************#
def get_tables_df(tables):
    num = tables.n
    d_df = {}
    if num != 0:
        for i in range(0,num):
            df_tb = tables[i].df
            df_tb = df_tb.replace('/na', '_', regex = True)
            colname = df_tb.iloc[0].str.replace('\n',' ',regex=True)
            df_tb.columns = colname
            df_tb = df_tb[1:]
            d_df[i] = df_tb
    else:
        return True
    return d_df
#********************************************************************************#
def extract_tables(list_of_files, df_slice):
    args = []
    for pdf_file in list_of_files:
        args.append([pdf_file, df_slice])
    return args
    
#    result = []
#    
#    # Multiprocessing execution mode    
#    with multiprocessing.Pool() as pool:
#        result = pool.map(mp.extract_table, args)
#    
#    # Sequential execution mode:
#    for arg in args:
#        result.append(mf.extract_table(arg))
#********************************************************************************#    
#Main Function
def extract_tables_noname(argument_list):  
    file = argument_list[0]
    df = argument_list[1]
    title_dict_f = dict()
    failed_pdf = []
    start_time = datetime.now()
#    file = '2949268.pdf'
    file_path = 'F:/Environmental Baseline Data/Version 4 - Final/PDF/{}'.format(file)

    print(file_path)
    file_name = file_path.split('/')[-1].replace('.pdf','')

    try:        
        pdf = PdfFileReader(open(file_path,'rb'))
        pages = pdf.getNumPages()
        print(pages)
        title_dict = dict()
        start_time = datetime.now()
        for ind in range(0,pages):
            pg_num = ind
            chars = []
            chars_final = []
            d_df = {}
            #camelot table objects for each page of the pdf
            try:
                tables = camelot.read_pdf(file_path, pages = str(pg_num+1), flag_size=True, copy_text=['v'],strip_text = '\n',line_scale=40, f = 'csv',flavour = 'stream')  #loop len(tables)
            except:
                print("camelot failed to extract table on page {} of {}".format(str(pg_num+1), file))
                continue
            #get table names in page == pg_num by parsing get_table_titles() function
            title_lst_raw = get_table_titles(pg_num+1,file,df)
            title_lst = replace_chars_strings(title_lst_raw)
            #get total number of table objects detected by Camelot in page == pg_num
            tb_num = tables.n
            d_df = get_tables_df(tables)
            print('{} tables on page {}'.format(str(tb_num),str(pg_num+1)))
            #VIEW
            print(title_lst)  
            #if Camelot returns NO table on the page continue the loop and go to the next page
            
            if tb_num == 0:
                print("No table on page "+ str(pg_num+1) + " is detected")
                continue
            #if whitespace of the detected table is larger than 69% of the entire table and there is only
            #one table on that page, identify this as figure and continute the loop and go to the next page
            elif tb_num == 1 and (tables[0].parsing_report)['whitespace'] > 69.0:
                print(f"Page {(tables[0].parsing_report)['page']} of the file {file} contains an image")  #add pdf ID!!!!
                continue
            #in case only one table is present on the page,
            elif tb_num == 1:
                print((tables[0].parsing_report)['whitespace'])
                df_tb = d_df[0] 
                #in case no title is extracted from this page but we know that there is one table
                if len(title_lst) == 0:
                    #let's say we are on page number x and this if statement checks whether page number x-1 contianed a table or not
                    #if the result is TRUE we assign the title of last table on previous page to this page's title-less table
                    print("YES 1")
                    if (pg_num-1 in title_dict):
                        print("NOT 2")
                        #find the list of tables on the previous page
                        lst_tbl = (title_dict.get(pg_num-1))[-1]
                        lst_tbl_df = lst_tbl[1]
                        #find similarity score of column names of table on page x and page x-1
                        col_concat_curr = list(df_tb.columns.values)
                        ccc = ''.join(col_concat_curr)
                        col_join_curr = (ccc.replace(' ','')).replace('\n','')
                        col_concat_prev = list(lst_tbl_df.columns.values)
                        ccp = ''.join(col_concat_prev)
                        col_join_prev = (ccp.replace(' ','')).replace('\n','')  
                        ratio_similarity = fuzz.token_sort_ratio(col_join_curr, col_join_prev)
                        #check if the columns of the last table on the previous page are the same as the table on this page
                        if (len((set(lst_tbl_df.columns)).difference(set(df_tb.columns))) == 0) or (len(set(lst_tbl_df.columns))== len(set(df_tb.columns))) or (ratio_similarity > 89):
                            xl_name = lst_tbl[2]
                            chars.append([0,df_tb,xl_name])
                            if file_name == xl_name:
                                xlsx_name = file_name + '_' +str(pg_num+1)+'_'+str(1)
                            else:
                                xlsx_name = file_name + '_' + xl_name +'_'+str(pg_num+1)+'_'+str(1)
                            xlsx_name_csv = file_name + '_' +str(pg_num+1)+'_'+str(1)+ '.csv'
                            chars_final.append([1,xlsx_name_csv, xlsx_name])
                            df_tb.to_csv(xlsx_name_csv, index = False, encoding='utf-8-sig')     
                        else:
                            xl_name = file_name
                            chars.append([0,df_tb,xl_name])
                            xlsx_name = file_name + '_' +str(pg_num+1)+'_'+str(1)
                            xlsx_name_csv = file_name + '_' +str(pg_num+1)+'_'+str(1)+ '.csv'
                            chars_final.append([1,xlsx_name_csv, xlsx_name])
                            df_tb.to_csv(xlsx_name_csv, index = False, encoding='utf-8-sig')     
                        title_dict[pg_num] = chars
                        title_dict_f[file_name+'-'+str(pg_num+1)]=chars_final

                    else:
                        xl_name = file_name    
                        chars.append([0,df_tb,xl_name])
                        xlsx_name = file_name + '_' +str(pg_num+1)+'_'+str(1)
                        xlsx_name_csv = file_name + '_' +str(pg_num+1)+'_'+str(1)+ '.csv'
                        chars_final.append([1,xlsx_name_csv,xlsx_name])
                        df_tb.to_csv(xlsx_name_csv, index = False, encoding='utf-8-sig')
                        title_dict[pg_num] = chars
                        title_dict_f[file_name+'-'+str(pg_num+1)]=chars_final
                else:
                    xl_name = title_lst[0]
                    #store page number, index of the table, and its name in a dictionary
                    chars.append([0,df_tb,xl_name])
                    xlsx_name = file_name + '_' + xl_name +'_'+str(pg_num+1)+'_'+str(1)
                    xlsx_name_csv = file_name + '_'+str(pg_num+1)+'_'+str(1)+ '.csv'
                    chars_final.append([1,xlsx_name_csv,xlsx_name])
                    df_tb.to_csv(xlsx_name_csv, index = False, encoding='utf-8-sig')
                    title_dict[pg_num] = chars
                    title_dict_f[file_name+'-'+str(pg_num+1)]=chars_final

            else:
                if len(title_lst) >= tb_num :
                    for j in range(0,tb_num):
                        df_tb = d_df[j]
                        xl_name = title_lst[j]
                        #store page number, index of the table, and its name in a dictionary
                        chars.append([j,df_tb,xl_name])
                        xlsx_name = file_name + '_' + xl_name +'_'+str(pg_num+1)+'_'+str(j+1)
                        xlsx_name_csv = file_name + '_'+str(pg_num+1)+'_'+str(j+1)+ '.csv'
                        chars_final.append([j+1,xlsx_name_csv,xlsx_name])
                        df_tb.to_csv(xlsx_name_csv, index = False, encoding='utf-8-sig')
                    title_dict[pg_num] = chars
                    title_dict_f[file_name+'-'+str(pg_num+1)]=chars_final

                elif len(title_lst) < tb_num :
                    try:
                        #first case: if the first table in the page is conitnuos of what was on the previous page
                        df_tb = d_df[0]
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

                            if len((set(lst_tbl_df.columns)).difference(set(df_tb.columns))) == 0 or len(set(lst_tbl_df.columns))== len(set(df_tb.columns)) or ratio_similarity > 89:
                                xl_name = lst_tbl[2]                           
                                chars.append([0,df_tb,xl_name]) 
                                if file_name == xl_name:
                                    xlsx_name = file_name + '_' +str(pg_num+1)+'_'+str(1)
                                else:
                                    xlsx_name = file_name + '_' + xl_name +'_'+str(pg_num+1)+'_'+str(1)
                                xlsx_name_csv = file_name + '_' +str(pg_num+1)+'_'+str(1)+ '.csv'
                                chars_final.append([1,xlsx_name_csv,xlsx_name])
                                df_tb.to_csv(xlsx_name_csv, index = False, encoding='utf-8-sig')   

                            else:
                                xl_name = file_name   
                                chars.append([0,df_tb,xl_name])
                                xlsx_name = file_name + '_' +str(pg_num+1)+'_'+str(1)
                                xlsx_name_csv = file_name + '_' +str(pg_num+1)+'_'+str(1)+ '.csv'
                                chars_final.append([1,xlsx_name_csv,xlsx_name])
                                df_tb.to_csv(xlsx_name_csv, index = False, encoding='utf-8-sig')
                        else:
                            xl_name = file_name       
                            chars.append([0,df_tb,xl_name])
                            xlsx_name = file_name + '_' +str(pg_num+1)+'_'+str(1)
                            xlsx_name_csv = file_name + '_' +str(pg_num+1)+'_'+str(1)+ '.csv'
                            chars_final.append([1,xlsx_name_csv,xlsx_name])
                            df_tb.to_csv(xlsx_name_csv, index = False, encoding='utf-8-sig')

                        indx = 0
                        for j in range(1,len(title_lst)+1):
                            df_tb = d_df[j]
                            xl_name = title_lst[indx]
                            indx = indx + 1
                            #store page number, index of the table, and its name in a dictionary
                            chars.append([j,df_tb,xl_name])
                            xlsx_name = file_name + '_' + xl_name +'_'+str(pg_num+1)+'_'+str(j+1)
                            xlsx_name_csv = file_name +'_'+str(pg_num+1)+'_'+str(j+1)+ '.csv'
                            chars_final.append([j+1,xlsx_name_csv,xlsx_name])
                            df_tb.to_csv(xlsx_name_csv, index = False, encoding='utf-8-sig')

                        for j in range(len(title_lst)+1,tb_num):
                            df_tb = d_df[j]
                            xl_name = file_name
                            #store page number, index of the table, and its name in a dictionary
                            chars.append([j,df_tb,xl_name])                        
                            #xlsx_name = file_name + '_' + xl_name +'_'+str(pg_num+1)+'_'+str(j+1)
                            xlsx_name = file_name + '_'+str(pg_num+1)+'_'+str(j+1)
                            xlsx_name_csv = file_name +'_'+str(pg_num+1)+'_'+str(j+1)+ '.csv'
                            chars_final.append([j+1,xlsx_name_csv,xlsx_name])
                            df_tb.to_csv(xlsx_name_csv, index = False, encoding='utf-8-sig')                    
                        title_dict[pg_num] = chars
                        title_dict_f[file_name+'-'+str(pg_num+1)]=chars_final

                    except:
                        print("Function failed on page {}".format(pg_num+1))
                        pass
    except:
        failed_pdf.append(file)
        print("file {} failed to open".format(file))
        pass
        
    end_time = datetime.now()
    print('Duration: {}'.format(end_time - start_time)) 
#    logging.info('Finished')
    
    
    with open('H:/GitHub/tmp/'+file_name + '_table-pages.txt', 'w+', encoding='utf-8') as output:
        output.write(json.dumps(title_dict_f, indent=2))
    
    return True
    
#
##********************************************************************************#
#def extract_table(argument_list):   
#    file = argument_list[0]
#    df = argument_list[1]
#    
#    failed_pdf = []
#    start_time = datetime.now()
#
#    file_path = 'F:/Environmental Baseline Data/Version 4 - Final/PDF/{}'.format(file)
#    file_name = file_path.split('/')[-1].replace('.pdf','')
#
#    try:
#        data = parser.from_file(file_path,xmlContent=True)
#        #raw_xml = parser.from_file('A6T2V6.pdf', xmlContent=True)
#        print(file_path)
#        #xml tag <div> splitting point for pages
#        soup = BeautifulSoup(data['content'], 'lxml')
#        pages = soup.find_all('div', attrs={'class': 'page'})
#        print(pages)
#        title_dict = dict()
#        start_time = datetime.now()
#        for ind, page in enumerate (pages):
#            pg_num = ind
#            chars = []
#            #camelot table objects for each page of the pdf
#            try:
#                tables = camelot.read_pdf(file_path, pages = str(pg_num+1), flag_size=True, copy_text=['v'],strip_text = '\n',line_scale=40, f = 'csv',flavour = 'stream')  #loop len(tables)
#            except:
#                continue
#            #get table names in page == pg_num by parsing get_table_titles() function
#            title_lst_raw = get_table_titles(pg_num+1,file,df)
#            title_lst = replace_chars_strings(title_lst_raw)
#            #get total number of table objects detected by Camelot in page == pg_num
#            tb_num = tables.n
#
#            #VIEW
#            print(title_lst)  
#            #if Camelot returns NO table on the page continue the loop and go to the next page
#            if tb_num == 0:
#                print("No table on page "+ str(pg_num+1) + " is detected")
#                continue
#            #if whitespace of the detected table is larger than 69% of the entire table and there is only
#            #one table on that page, identify this as figure and continute the loop and go to the next page
#            elif tb_num == 1 and (tables[0].parsing_report)['whitespace'] > 69.0:
#                print(f"Page {(tables[0].parsing_report)['page']} contains an image")  #add pdf ID!!!!
#                continue
#            
#            #************
##            elif ca.is_alignmentsheet(file_name,pg_num):
##                print("Page {} of file {} contains is detected as an alignmentsheet".format(str(pg_num+1),file_name))
##                continue
#            #***********
#            
#            #in case only one table is present on the page,
#            elif tb_num == 1:
#                #this block distills the dataframe with proper column names
#                df_tb = tables[0].df
#                df_tb = df_tb.replace('/na', '_', regex = True)
#                colname = df_tb.iloc[0].str.replace('\n',' ',regex=True)
#                df_tb.columns = colname
#                df_tb = df_tb[1:]   
#                #df_tb = df_tb.iloc[1:]
##                if df_tb.empty:
##                    continue
##                else:
#                #in case no title is extracted from this page but we know that there is one table
#                if len(title_lst) == 0:
#                    #let's say we are on page number x and this if statement checks whether page number x-1 contianed a table or not
#                    #if the result is TRUE we assign the title of last table on previous page to this page's title-less table
#                    if (pg_num-1 in title_dict):
#                        #find the list of tables on the previous page
#                        lst_tbl = (title_dict.get(pg_num-1))[-1]
#                        lst_tbl_df = lst_tbl[1]
#                        #find similarity score of column names of table on page x and page x-1
#                        col_concat_curr = list(df_tb.columns.values)
#                        ccc = ''.join(col_concat_curr)
#                        col_join_curr = (ccc.replace(' ','')).replace('\n','')
#                        col_concat_prev = list(lst_tbl_df.columns.values)
#                        ccp = ''.join(col_concat_prev)
#                        col_join_prev = (ccp.replace(' ','')).replace('\n','')  
#                        ratio_similarity = fuzz.token_sort_ratio(col_join_curr, col_join_prev)
#                        #check if the columns of the last table on the previous page are the same as the table on this page
#                        if (len((set(lst_tbl_df.columns)).difference(set(df_tb.columns))) == 0) or (len(set(lst_tbl_df.columns))== len(set(df_tb.columns))) or (ratio_similarity > 89):
#                            xl_name = lst_tbl[2]
#                            chars.append([0,df_tb,xl_name])
#                            xlsx_name = file_name + '_' + xl_name +'_'+str(pg_num+1)+'_'+str(1)+ '.csv'
#                            df_tb.to_csv(xlsx_name, index = False, encoding='utf-8-sig')     
#                        else:
#                            xl_name = file_name
#                            chars.append([0,df_tb,xl_name])
#                            xlsx_name = file_name + '_' +str(pg_num+1)+'_'+str(1)+ '.csv'
#                            df_tb.to_csv(xlsx_name, index = False, encoding='utf-8-sig')
#                        title_dict[pg_num] = chars
#
#                    else:
#                        xl_name = file_name       
#                        chars.append([0,df_tb,xl_name])
#                        xlsx_name = file_name + '_' +str(pg_num+1)+'_'+str(1)+ '.csv'
#                        df_tb.to_csv(xlsx_name, index = False, encoding='utf-8-sig')
#                        title_dict[pg_num] = chars
#                else:
#                    xl_name = title_lst[0]
#        #                xl_name = xl_name.replace('/','_')
#        #                xl_name = xl_name.replace(':','')
#                    #store page number, index of the table, and its name in a dictionary
#                    chars.append([0,df_tb,xl_name])
#                    xlsx_name = file_name + '_' + xl_name +'_'+str(pg_num+1)+'_'+str(1)+ '.csv'
#                    df_tb.to_csv(xlsx_name, index = False, encoding='utf-8-sig')
#                    title_dict[pg_num] = chars
#                    
#            elif len(title_lst) >= tb_num :
#                for j in range(0,tb_num):
#                    df_tb = tables[j].df
#                    df_tb = df_tb.replace('/na', '_', regex = True)
#                    df_tb.columns = df_tb.iloc[0].str.replace('\n',' ',regex=True)
#                    df_tb = df_tb[1:]
#                    if df_tb.empty:
#                        continue
#                    else:
#                        #df_tb = df_tb.iloc[1:]
#                        xl_name = title_lst[j]
#        #                    xl_name = xl_name.replace('/','_')
#        #                    xl_name = xl_name.replace(':','')
#    
#                        #store page number, index of the table, and its name in a dictionary
#                        chars.append([j,df_tb,xl_name])
#    
#                        xlsx_name = file_name + '_' + xl_name +'_'+str(pg_num+1)+'_'+str(j+1)+ '.csv'
#                        df_tb.to_csv(xlsx_name, index = False, encoding='utf-8-sig')
#
#                title_dict[pg_num] = chars
#
#            elif len(title_lst) < tb_num :
#                try:
#                    #first case: if the first table in the page is conitnuos of what was on the previous page
#                    df_tb = tables[0].df
#                    df_tb = df_tb.replace('/na', '_', regex = True)
#                    df_tb.columns = df_tb.iloc[0].str.replace('\n',' ',regex=True)
#                    df_tb = df_tb[1:]
#                    #df_tb = df_tb.iloc[1:]
#
#                    if (pg_num-1 in title_dict):
#                        #find the list of tables on the previous page
#                        lst_tbl = (title_dict.get(pg_num-1))[-1]
#                        lst_tbl_df = lst_tbl[1]
#                        #check if the columns of the last table on the previous page are the same as the table on this page
#
#                        col_concat_curr = list(df_tb.columns.values)
#                        ccc = ''.join(col_concat_curr)
#                        col_join_curr = (ccc.replace(' ','')).replace('\n','')
#                        col_concat_prev = list(lst_tbl_df.columns.values)
#                        ccp = ''.join(col_concat_prev)
#                        col_join_prev = (ccp.replace(' ','')).replace('\n','')  
#                        ratio_similarity = fuzz.token_sort_ratio(ccc, ccp)
#
#                        if len((set(lst_tbl_df.columns)).difference(set(df_tb.columns))) == 0 or len(set(lst_tbl_df.columns))== len(set(df_tb.columns)) or ratio_similarity > 89:
#                            xl_name = lst_tbl[2]                           
#                            chars.append([0,df_tb,xl_name])                         
#                            xlsx_name = file_name + '_' + xl_name +'_'+str(pg_num+1)+'_'+str(1)+ '.csv'
#                            df_tb.to_csv(xlsx_name, index = False, encoding='utf-8-sig')
#
#                        else:
#                            xl_name = file_name   
#                            chars.append([0,df_tb,xl_name])
#                            xlsx_name = file_name + '_' +str(pg_num+1)+'_'+str(1)+ '.csv'
#                            df_tb.to_csv(xlsx_name, index = False, encoding='utf-8-sig')
#                    else:
#                        xl_name = file_name       
#                        chars.append([0,df_tb,xl_name])
#                        xlsx_name = file_name + '_' +str(pg_num+1)+'_'+str(1)+ '.csv'
#                        df_tb.to_csv(xlsx_name, index = False, encoding='utf-8-sig')
#
#                    indx = 0
#                    for j in range(1,len(title_lst)+1):
#                        df_tb = tables[j].df
#                        df_tb = df_tb.replace('/na', '_', regex = True)
#                        df_tb.columns = df_tb.iloc[0].str.replace('\n',' ',regex=True)
#                        df_tb = df_tb[1:]
#                        #df_tb = df_tb.iloc[1:]
#                        xl_name = title_lst[indx]
#                        indx = indx + 1
#    #                        xl_name = xl_name.replace('/','_')
#    #                        xl_name = xl_name.replace(':','')
#                        #store page number, index of the table, and its name in a dictionary
#                        chars.append([j,df_tb,xl_name])
#
#                        xlsx_name = file_name + '_' + xl_name +'_'+str(pg_num+1)+'_'+str(j+1)+ '.csv'
#                        #xlsx_name = z.split('\\')[-1] + '-' + str(pg[i]+1) + str(j) + '.xlsx'
#                        df_tb.to_csv(xlsx_name, index = False, encoding='utf-8-sig')
#
#                    for j in range(len(title_lst)+1,tb_num):
#                        df_tb = tables[j].df
#                        df_tb = df_tb.replace('/na', '_', regex = True)
#                        df_tb.columns = df_tb.iloc[0].str.replace('\n',' ',regex=True)
#                        df_tb = df_tb[1:]
#                        #df_tb = df_tb.iloc[1:]
#                        xl_name = file_name
#                        #store page number, index of the table, and its name in a dictionary
#                        chars.append([j,df_tb,xl_name])                        
#
#                        xlsx_name = file_name + '_' + xl_name +'_'+str(pg_num+1)+'_'+str(j+1)+ '.csv'
#                        #xlsx_name = z.split('\\')[-1] + '-' + str(pg[i]+1) + str(j) + '.xlsx'
#                        df_tb.to_csv(xlsx_name, index = False, encoding='utf-8-sig')                    
#                    title_dict[pg_num] = chars
#    
#                except:
#                    print("Function failed on page {}".format(pg_num+1))
#                    pass
#    except:
#        failed_pdf.append(file)
#        print("file {} failed to open".format(file))
#        pass
#        
#    end_time = datetime.now()
#    print('Duration: {}'.format(end_time - start_time)) 
#    
#    return failed_pdf
#
#
#
#
#################SEQUENTIAL########################
#def extract_table_(file,df):   
#    #file = argument_list[0]
#    #df = argument_list[1]
#    
#    failed_pdf = []
#    start_time = datetime.now()
#
#    file_path = 'F:/Environmental Baseline Data/Version 4 - Final/PDF/{}'.format(file)
#
#    print(file_path)
#    file_name = file_path.split('/')[-1].replace('.pdf','')
#
#
#    try:
#        data = parser.from_file(file_path,xmlContent=True)
#        #raw_xml = parser.from_file('A6T2V6.pdf', xmlContent=True)
#        print(file_path)
#        #xml tag <div> splitting point for pages
#        soup = BeautifulSoup(data['content'], 'lxml')
#        pages = soup.find_all('div', attrs={'class': 'page'})
#
#        title_dict = dict()
#        start_time = datetime.now()
#        for ind, page in enumerate (pages):
#            pg_num = ind
#            chars = []
#            #camelot table objects for each page of the pdf
#            try:
#                tables = camelot.read_pdf(file_path, pages = str(pg_num+1), flag_size=True, copy_text=['v'],strip_text = '\n',line_scale=40, f = 'csv',flavour = 'stream')  #loop len(tables)
#            except:
#                continue
#            #get table names in page == pg_num by parsing get_table_titles() function
#            title_lst_raw = get_table_titles(pg_num+1,file,df)
#            title_lst = replace_chars_strings(title_lst_raw)
#            #get total number of table objects detected by Camelot in page == pg_num
#            tb_num = tables.n
#
#            #VIEW
#            print(title_lst)  
#            #if Camelot returns NO table on the page continue the loop and go to the next page
#            if tb_num == 0:
#                print("No table on page "+ str(pg_num+1) + " is detected")
#                continue
#            #if whitespace of the detected table is larger than 69% of the entire table and there is only
#            #one table on that page, identify this as figure and continute the loop and go to the next page
#            elif tb_num == 1 and (tables[0].parsing_report)['whitespace'] > 69.0:
#                print(f"Page {(tables[0].parsing_report)['page']} contains an image")  #add pdf ID!!!!
#                continue
#            
#            #************
##            elif ca.is_alignmentsheet(file_name,pg_num):
##                print("Page {} of file {} contains is detected as an alignmentsheet".format(str(pg_num+1),file_name))
##                continue
#            #***********
#            
#            #in case only one table is present on the page,
#            elif tb_num == 1:
#                #this block distills the dataframe with proper column names
#                df_tb = tables[0].df
#                df_tb = df_tb.replace('/na', '_', regex = True)
#                colname = df_tb.iloc[0].str.replace('\n',' ',regex=True)
#                df_tb.columns = colname
#                df_tb = df_tb[1:]   
#                #df_tb = df_tb.iloc[1:]
##                if df_tb.empty:
##                    continue
##                else:
#                #in case no title is extracted from this page but we know that there is one table
#                if len(title_lst) == 0:
#                    #let's say we are on page number x and this if statement checks whether page number x-1 contianed a table or not
#                    #if the result is TRUE we assign the title of last table on previous page to this page's title-less table
#                    if (pg_num-1 in title_dict):
#                        #find the list of tables on the previous page
#                        lst_tbl = (title_dict.get(pg_num-1))[-1]
#                        lst_tbl_df = lst_tbl[1]
#                        #find similarity score of column names of table on page x and page x-1
#                        col_concat_curr = list(df_tb.columns.values)
#                        ccc = ''.join(col_concat_curr)
#                        col_join_curr = (ccc.replace(' ','')).replace('\n','')
#                        col_concat_prev = list(lst_tbl_df.columns.values)
#                        ccp = ''.join(col_concat_prev)
#                        col_join_prev = (ccp.replace(' ','')).replace('\n','')  
#                        ratio_similarity = fuzz.token_sort_ratio(col_join_curr, col_join_prev)
#                        #check if the columns of the last table on the previous page are the same as the table on this page
#                        if (len((set(lst_tbl_df.columns)).difference(set(df_tb.columns))) == 0) or (len(set(lst_tbl_df.columns))== len(set(df_tb.columns))) or (ratio_similarity > 89):
#                            xl_name = lst_tbl[2]
#                            chars.append([0,df_tb,xl_name])
#                            xlsx_name = file_name + '_' + xl_name +'_'+str(pg_num+1)+'_'+str(1)+ '.csv'
#                            df_tb.to_csv(xlsx_name, index = False, encoding='utf-8-sig')     
#                        else:
#                            xl_name = file_name
#                            chars.append([0,df_tb,xl_name])
#                            xlsx_name = file_name + '_' +str(pg_num+1)+'_'+str(1)+ '.csv'
#                            df_tb.to_csv(xlsx_name, index = False, encoding='utf-8-sig')
#                        title_dict[pg_num] = chars
#
#                    else:
#                        xl_name = file_name       
#                        chars.append([0,df_tb,xl_name])
#                        xlsx_name = file_name + '_' +str(pg_num+1)+'_'+str(1)+ '.csv'
#                        df_tb.to_csv(xlsx_name, index = False, encoding='utf-8-sig')
#                        title_dict[pg_num] = chars
#                else:
#                    xl_name = title_lst[0]
#        #                xl_name = xl_name.replace('/','_')
#        #                xl_name = xl_name.replace(':','')
#                    #store page number, index of the table, and its name in a dictionary
#                    chars.append([0,df_tb,xl_name])
#                    xlsx_name = file_name + '_' + xl_name +'_'+str(pg_num+1)+'_'+str(1)+ '.csv'
#                    df_tb.to_csv(xlsx_name, index = False, encoding='utf-8-sig')
#                    title_dict[pg_num] = chars
#                    
#            elif len(title_lst) >= tb_num :
#                for j in range(0,tb_num):
#                    df_tb = tables[j].df
#                    df_tb = df_tb.replace('/na', '_', regex = True)
#                    df_tb.columns = df_tb.iloc[0].str.replace('\n',' ',regex=True)
#                    df_tb = df_tb[1:]
#                    if df_tb.empty:
#                        continue
#                    else:
#                        #df_tb = df_tb.iloc[1:]
#                        xl_name = title_lst[j]
#        #                    xl_name = xl_name.replace('/','_')
#        #                    xl_name = xl_name.replace(':','')
#    
#                        #store page number, index of the table, and its name in a dictionary
#                        chars.append([j,df_tb,xl_name])
#    
#                        xlsx_name = file_name + '_' + xl_name +'_'+str(pg_num+1)+'_'+str(j+1)+ '.csv'
#                        df_tb.to_csv(xlsx_name, index = False, encoding='utf-8-sig')
#
#                title_dict[pg_num] = chars
#
#            elif len(title_lst) < tb_num :
#                try:
#                    #first case: if the first table in the page is conitnuos of what was on the previous page
#                    df_tb = tables[0].df
#                    df_tb = df_tb.replace('/na', '_', regex = True)
#                    df_tb.columns = df_tb.iloc[0].str.replace('\n',' ',regex=True)
#                    df_tb = df_tb[1:]
#                    #df_tb = df_tb.iloc[1:]
#
#                    if (pg_num-1 in title_dict):
#                        #find the list of tables on the previous page
#                        lst_tbl = (title_dict.get(pg_num-1))[-1]
#                        lst_tbl_df = lst_tbl[1]
#                        #check if the columns of the last table on the previous page are the same as the table on this page
#
#                        col_concat_curr = list(df_tb.columns.values)
#                        ccc = ''.join(col_concat_curr)
#                        col_join_curr = (ccc.replace(' ','')).replace('\n','')
#                        col_concat_prev = list(lst_tbl_df.columns.values)
#                        ccp = ''.join(col_concat_prev)
#                        col_join_prev = (ccp.replace(' ','')).replace('\n','')  
#                        ratio_similarity = fuzz.token_sort_ratio(ccc, ccp)
#
#                        if len((set(lst_tbl_df.columns)).difference(set(df_tb.columns))) == 0 or len(set(lst_tbl_df.columns))== len(set(df_tb.columns)) or ratio_similarity > 89:
#                            xl_name = lst_tbl[2]                           
#                            chars.append([0,df_tb,xl_name])                         
#                            xlsx_name = file_name + '_' + xl_name +'_'+str(pg_num+1)+'_'+str(1)+ '.csv'
#                            df_tb.to_csv(xlsx_name, index = False, encoding='utf-8-sig')
#
#                        else:
#                            xl_name = file_name   
#                            chars.append([0,df_tb,xl_name])
#                            xlsx_name = file_name + '_' +str(pg_num+1)+'_'+str(1)+ '.csv'
#                            df_tb.to_csv(xlsx_name, index = False, encoding='utf-8-sig')
#                    else:
#                        xl_name = file_name       
#                        chars.append([0,df_tb,xl_name])
#                        xlsx_name = file_name + '_' +str(pg_num+1)+'_'+str(1)+ '.csv'
#                        df_tb.to_csv(xlsx_name, index = False, encoding='utf-8-sig')
#
#                    indx = 0
#                    for j in range(1,len(title_lst)+1):
#                        df_tb = tables[j].df
#                        df_tb = df_tb.replace('/na', '_', regex = True)
#                        df_tb.columns = df_tb.iloc[0].str.replace('\n',' ',regex=True)
#                        df_tb = df_tb[1:]
#                        #df_tb = df_tb.iloc[1:]
#                        xl_name = title_lst[indx]
#                        indx = indx + 1
#    #                        xl_name = xl_name.replace('/','_')
#    #                        xl_name = xl_name.replace(':','')
#                        #store page number, index of the table, and its name in a dictionary
#                        chars.append([j,df_tb,xl_name])
#
#                        xlsx_name = file_name + '_' + xl_name +'_'+str(pg_num+1)+'_'+str(j+1)+ '.csv'
#                        #xlsx_name = z.split('\\')[-1] + '-' + str(pg[i]+1) + str(j) + '.xlsx'
#                        df_tb.to_csv(xlsx_name, index = False, encoding='utf-8-sig')
#
#                    for j in range(len(title_lst)+1,tb_num):
#                        df_tb = tables[j].df
#                        df_tb = df_tb.replace('/na', '_', regex = True)
#                        df_tb.columns = df_tb.iloc[0].str.replace('\n',' ',regex=True)
#                        df_tb = df_tb[1:]
#                        #df_tb = df_tb.iloc[1:]
#                        xl_name = file_name
#                        #store page number, index of the table, and its name in a dictionary
#                        chars.append([j,df_tb,xl_name])                        
#
#                        xlsx_name = file_name + '_' + xl_name +'_'+str(pg_num+1)+'_'+str(j+1)+ '.csv'
#                        #xlsx_name = z.split('\\')[-1] + '-' + str(pg[i]+1) + str(j) + '.xlsx'
#                        df_tb.to_csv(xlsx_name, index = False, encoding='utf-8-sig')                    
#                    title_dict[pg_num] = chars
#    
#                except:
#                    print("Function failed on page {}".format(pg_num+1))
#                    pass
#    except:
#        failed_pdf.append(file)
#        print("file {} failed to open".format(file))
#        pass
#        
#    end_time = datetime.now()
#    print('Duration: {}'.format(end_time - start_time)) 
#    
#    return failed_pdf
#
#############################################################################################################

#
##Main Function
#def extract_tables_noname(file:str(),df:pd.DataFrame,title_dict_f:dict()) -> dict:   
##    logging.basicConfig(filename='myapp.log', level=logging.INFO)
##    logging.info('Started')
#    failed_pdf = []
#    start_time = datetime.now()
##    file = '2949268.pdf'
#    file_path = 'F:/Environmental Baseline Data/Version 4 - Final/PDF/{}'.format(file)
#
#    print(file_path)
#    file_name = file_path.split('/')[-1].replace('.pdf','')
#
#    try:        
#        pdf = PdfFileReader(open(file_path,'rb'))
#        pages = pdf.getNumPages()
#        print(pages)
#        title_dict = dict()
#        start_time = datetime.now()
#        for ind in range(0,pages):
#            pg_num = ind
#            chars = []
#            chars_final = []
#            d_df = {}
#            #camelot table objects for each page of the pdf
#            try:
#                tables = camelot.read_pdf(file_path, pages = str(pg_num+1), flag_size=True, copy_text=['v'],strip_text = '\n',line_scale=40, f = 'csv',flavour = 'stream')  #loop len(tables)
#            except:
#                print("camelot failed to extract table on page {} of {}".format(str(pg_num+1), file))
#                continue
#            #get table names in page == pg_num by parsing get_table_titles() function
#            title_lst_raw = get_table_titles(pg_num+1,file,df)
#            title_lst = replace_chars_strings(title_lst_raw)
#            #get total number of table objects detected by Camelot in page == pg_num
#            tb_num = tables.n
#            d_df = get_tables_df(tables)
#            print('{} tables on page {}'.format(str(tb_num),str(pg_num+1)))
#            #VIEW
#            print(title_lst)  
#            #if Camelot returns NO table on the page continue the loop and go to the next page
#            
#            if tb_num == 0:
#                print("No table on page "+ str(pg_num+1) + " is detected")
#                continue
#            #if whitespace of the detected table is larger than 69% of the entire table and there is only
#            #one table on that page, identify this as figure and continute the loop and go to the next page
#            elif tb_num == 1 and (tables[0].parsing_report)['whitespace'] > 69.0:
#                print(f"Page {(tables[0].parsing_report)['page']} of the file {file} contains an image")  #add pdf ID!!!!
#                continue
#            #in case only one table is present on the page,
#            elif tb_num == 1:
#                print((tables[0].parsing_report)['whitespace'])
#                df_tb = d_df[0] 
#                #in case no title is extracted from this page but we know that there is one table
#                if len(title_lst) == 0:
#                    #let's say we are on page number x and this if statement checks whether page number x-1 contianed a table or not
#                    #if the result is TRUE we assign the title of last table on previous page to this page's title-less table
#                    print("YES 1")
#                    if (pg_num-1 in title_dict):
#                        print("NOT 2")
#                        #find the list of tables on the previous page
#                        lst_tbl = (title_dict.get(pg_num-1))[-1]
#                        lst_tbl_df = lst_tbl[1]
#                        #find similarity score of column names of table on page x and page x-1
#                        col_concat_curr = list(df_tb.columns.values)
#                        ccc = ''.join(col_concat_curr)
#                        col_join_curr = (ccc.replace(' ','')).replace('\n','')
#                        col_concat_prev = list(lst_tbl_df.columns.values)
#                        ccp = ''.join(col_concat_prev)
#                        col_join_prev = (ccp.replace(' ','')).replace('\n','')  
#                        ratio_similarity = fuzz.token_sort_ratio(col_join_curr, col_join_prev)
#                        #check if the columns of the last table on the previous page are the same as the table on this page
#                        if (len((set(lst_tbl_df.columns)).difference(set(df_tb.columns))) == 0) or (len(set(lst_tbl_df.columns))== len(set(df_tb.columns))) or (ratio_similarity > 89):
#                            xl_name = lst_tbl[2]
#                            chars.append([0,df_tb,xl_name])
#                            if file_name == xl_name:
#                                xlsx_name = file_name + '_' +str(pg_num+1)+'_'+str(1)
#                            else:
#                                xlsx_name = file_name + '_' + xl_name +'_'+str(pg_num+1)+'_'+str(1)
#                            xlsx_name_csv = file_name + '_' +str(pg_num+1)+'_'+str(1)+ '.csv'
#                            chars_final.append([1,xlsx_name_csv, xlsx_name])
#                            df_tb.to_csv(xlsx_name_csv, index = False, encoding='utf-8-sig')     
#                        else:
#                            xl_name = file_name
#                            chars.append([0,df_tb,xl_name])
#                            xlsx_name = file_name + '_' +str(pg_num+1)+'_'+str(1)
#                            xlsx_name_csv = file_name + '_' +str(pg_num+1)+'_'+str(1)+ '.csv'
#                            chars_final.append([1,xlsx_name_csv, xlsx_name])
#                            df_tb.to_csv(xlsx_name_csv, index = False, encoding='utf-8-sig')     
#                        title_dict[pg_num] = chars
#                        title_dict_f[file_name+'-'+str(pg_num+1)]=chars_final
#
#                    else:
#                        xl_name = file_name    
#                        chars.append([0,df_tb,xl_name])
#                        xlsx_name = file_name + '_' +str(pg_num+1)+'_'+str(1)
#                        xlsx_name_csv = file_name + '_' +str(pg_num+1)+'_'+str(1)+ '.csv'
#                        chars_final.append([1,xlsx_name_csv,xlsx_name])
#                        df_tb.to_csv(xlsx_name_csv, index = False, encoding='utf-8-sig')
#                        title_dict[pg_num] = chars
#                        title_dict_f[file_name+'-'+str(pg_num+1)]=chars_final
#                else:
#                    xl_name = title_lst[0]
#                    #store page number, index of the table, and its name in a dictionary
#                    chars.append([0,df_tb,xl_name])
#                    xlsx_name = file_name + '_' + xl_name +'_'+str(pg_num+1)+'_'+str(1)
#                    xlsx_name_csv = file_name + '_'+str(pg_num+1)+'_'+str(1)+ '.csv'
#                    chars_final.append([1,xlsx_name_csv,xlsx_name])
#                    df_tb.to_csv(xlsx_name_csv, index = False, encoding='utf-8-sig')
#                    title_dict[pg_num] = chars
#                    title_dict_f[file_name+'-'+str(pg_num+1)]=chars_final
#
#            else:
#                if len(title_lst) >= tb_num :
#                    for j in range(0,tb_num):
#                        df_tb = d_df[j]
#                        xl_name = title_lst[j]
#                        #store page number, index of the table, and its name in a dictionary
#                        chars.append([j,df_tb,xl_name])
#                        xlsx_name = file_name + '_' + xl_name +'_'+str(pg_num+1)+'_'+str(j+1)
#                        xlsx_name_csv = file_name + '_'+str(pg_num+1)+'_'+str(j+1)+ '.csv'
#                        chars_final.append([j+1,xlsx_name_csv,xlsx_name])
#                        df_tb.to_csv(xlsx_name_csv, index = False, encoding='utf-8-sig')
#                    title_dict[pg_num] = chars
#                    title_dict_f[file_name+'-'+str(pg_num+1)]=chars_final
#
#                elif len(title_lst) < tb_num :
#                    try:
#                        #first case: if the first table in the page is conitnuos of what was on the previous page
#                        df_tb = d_df[0]
#                        if (pg_num-1 in title_dict):
#                            #find the list of tables on the previous page
#                            lst_tbl = (title_dict.get(pg_num-1))[-1]
#                            lst_tbl_df = lst_tbl[1]
#                            #check if the columns of the last table on the previous page are the same as the table on this page
#                            col_concat_curr = list(df_tb.columns.values)
#                            ccc = ''.join(col_concat_curr)
#                            col_join_curr = (ccc.replace(' ','')).replace('\n','')
#                            col_concat_prev = list(lst_tbl_df.columns.values)
#                            ccp = ''.join(col_concat_prev)
#                            col_join_prev = (ccp.replace(' ','')).replace('\n','')  
#                            ratio_similarity = fuzz.token_sort_ratio(ccc, ccp)
#
#                            if len((set(lst_tbl_df.columns)).difference(set(df_tb.columns))) == 0 or len(set(lst_tbl_df.columns))== len(set(df_tb.columns)) or ratio_similarity > 89:
#                                xl_name = lst_tbl[2]                           
#                                chars.append([0,df_tb,xl_name]) 
#                                if file_name == xl_name:
#                                    xlsx_name = file_name + '_' +str(pg_num+1)+'_'+str(1)
#                                else:
#                                    xlsx_name = file_name + '_' + xl_name +'_'+str(pg_num+1)+'_'+str(1)
#                                xlsx_name_csv = file_name + '_' +str(pg_num+1)+'_'+str(1)+ '.csv'
#                                chars_final.append([1,xlsx_name_csv,xlsx_name])
#                                df_tb.to_csv(xlsx_name_csv, index = False, encoding='utf-8-sig')   
#
#                            else:
#                                xl_name = file_name   
#                                chars.append([0,df_tb,xl_name])
#                                xlsx_name = file_name + '_' +str(pg_num+1)+'_'+str(1)
#                                xlsx_name_csv = file_name + '_' +str(pg_num+1)+'_'+str(1)+ '.csv'
#                                chars_final.append([1,xlsx_name_csv,xlsx_name])
#                                df_tb.to_csv(xlsx_name_csv, index = False, encoding='utf-8-sig')
#                        else:
#                            xl_name = file_name       
#                            chars.append([0,df_tb,xl_name])
#                            xlsx_name = file_name + '_' +str(pg_num+1)+'_'+str(1)
#                            xlsx_name_csv = file_name + '_' +str(pg_num+1)+'_'+str(1)+ '.csv'
#                            chars_final.append([1,xlsx_name_csv,xlsx_name])
#                            df_tb.to_csv(xlsx_name_csv, index = False, encoding='utf-8-sig')
#
#                        indx = 0
#                        for j in range(1,len(title_lst)+1):
#                            df_tb = d_df[j]
#                            xl_name = title_lst[indx]
#                            indx = indx + 1
#                            #store page number, index of the table, and its name in a dictionary
#                            chars.append([j,df_tb,xl_name])
#                            xlsx_name = file_name + '_' + xl_name +'_'+str(pg_num+1)+'_'+str(j+1)
#                            xlsx_name_csv = file_name +'_'+str(pg_num+1)+'_'+str(j+1)+ '.csv'
#                            chars_final.append([j+1,xlsx_name_csv,xlsx_name])
#                            df_tb.to_csv(xlsx_name_csv, index = False, encoding='utf-8-sig')
#
#                        for j in range(len(title_lst)+1,tb_num):
#                            df_tb = d_df[j]
#                            xl_name = file_name
#                            #store page number, index of the table, and its name in a dictionary
#                            chars.append([j,df_tb,xl_name])                        
#                            xlsx_name = file_name + '_' + xl_name +'_'+str(pg_num+1)+'_'+str(j+1)
#                            xlsx_name_csv = file_name +'_'+str(pg_num+1)+'_'+str(j+1)+ '.csv'
#                            chars_final.append([j+1,xlsx_name_csv,xlsx_name])
#                            df_tb.to_csv(xlsx_name_csv, index = False, encoding='utf-8-sig')                    
#                        title_dict[pg_num] = chars
#                        title_dict_f[file_name+'-'+str(pg_num+1)]=chars_final
#
#                    except:
#                        print("Function failed on page {}".format(pg_num+1))
#                        pass
#    except:
#        failed_pdf.append(file)
#        print("file {} failed to open".format(file))
#        pass
#        
#    end_time = datetime.now()
#    print('Duration: {}'.format(end_time - start_time)) 
##    logging.info('Finished')
#    
#    return title_dict_f
#    
#hearing_all = ['Application for the Albersun Pipeline Asset Purchase G','Application for the Alberta Clipper Expansion Project','Application for Bakken Pipeline Project Canada','Application for the Brunswick Pipeline Project','Application for the Wolverine River Lateral Loop Carmon Creek Section','Application for the Cushing Expansion','Application for Leave to Abandon Deep Panuke Pipeline','Applications for Energy East, Asset Transfer and Eastern Mainline  Eastern Mainline ESA','Application for the Edmonton to Hardisty Pipeline Project','Application for the Construction of Edson Mainline Expansion Project','Application to Construct and Operate Ekwan Pipeline','Applications for Energy East, Asset Transfer and Eastern Mainline','Application for the Enbridge Northern Gateway Pipeline Project','Application for the Goldboro Gas Plant and 26" Gathering Pipeline Abandonment','Application for the Groundbirch Pipeline Project','Application for the Horn River Project','Application for the Keystone XL Pipeline','Application for Northwest Mainline Komie North Extension','Application for the Leismer to Kettle River Crossover','Application for the Line 3 Replacement Program','Application for the Line 4 Extension Project','Application for Line 9 Reversal Phase I Project','Application for the 2017 NGTL System Expansion','Application for 2021 NGTL System Expansion Project','Application for the Construction of North Corridor Expansion Project','Application for North Montney Project','Application for the Northwest Mainline Expansion','Application for Redwillow Pipeline Project','Application to construct and operate the South Peace Pipeline Project','Application for Line 13 Transfer, Line 13 Reversal and Capacity Replacement for the Southern Lights Project','Application for the Spruce Ridge Program','Application for the Towerbirch Expansion Project','Application for Trans Mountain Expansion Project', 'Application for the Vantage Pipeline Project','Application for the construction of the West Path Delivery Project','Application for the Wyndwood Pipeline Expansion Project']
#
#
#folder_names = ['albersun','alberta_clipper','bakken','brunswick','carmon_creek','cushing_expansion','deep_panuke','eastern_mainline','edmonton_hardisty','edson_mainline','ekwan','energy_east','gateway','goldboro_abandonment','horn_river',
#           'groundbirch','keystoneXL','komie_north','leismer2kettle','line3','line4','line9','ngtl2017','ngtl2021','north_corridor','north_montney','northwest_mainline','red_willow',
#           'south_peace','southern_lights','spruce_ridge','towerbirch','trans_mountain','vantage','west_path','wyndwood']
#
#folder_names = ['trans_mountain']
#hearing_all = ['Application for Trans Mountain Expansion Project']
##['Application for the Vantage Pipeline Project',
##Dataframe with all the table names
#path = 'F:/Environmental Baseline Data/Version 4 - Final/Support files/Table titles raw data/final_table_titles8.csv'
#df = pd.read_csv(path, usecols = ['page_number','final_table_title', 'Application title short', 'DataID_pdf','categories', 'Category'])
#df = df[df['categories'] > 0] 
#df = df[df['Category'] == 'Table']
#df['final_table_title'] = df['final_table_title'].str.title()
#df.head()
#
#
#import sys
#    
#if __name__ == '__main__':
##    os.chdir("H:/GitHub")
##    sys.stdout = open("file.txt","w")
#    global title_dict_final 
#    title_dict_final = dict()
#    # ## Application for 2021 NGTL System Expansion Project
#    for ind, item in enumerate (hearing_all):
#        hearing = item
#        df_by_project = df[df['Application title short'] == hearing].reset_index(drop = True)
#        
#        dirName = r'F:\Environmental Baseline Data\Version 4 - Final\CSV3\{}'.format(folder_names[ind])
#        
#        if not os.path.exists(dirName):
#            os.mkdir(dirName)
#            print("Directory " , dirName ,  " Created ")
#        else:    
#            print("Directory " , dirName ,  " already exists")
#            
#        os.chdir(dirName)    
#       
#        files = list(df_by_project['DataID_pdf'].unique())
#        for file in files:
#            extract_tables_noname(file, df_by_project,title_dict_final)
#
##last_2 = title_dict_final
##trans_m = title_dict_final
#import json
#with open("transm_table-pages.txt", 'w+', encoding='utf-8') as output:
#    output.write(json.dumps(title_dict_final, indent=2))       
#























