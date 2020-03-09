#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interactivity = "all"


# In[2]:


#WHERE WE EXTRACT TABLE NAME FROM CSV
from bs4 import BeautifulSoup
from tika import parser
import os
import re
import camelot
from fuzzywuzzy import fuzz
from datetime import datetime
import textwrap
import multiprocessing

# In[3]:


#######################FUNCTIONS##################################################
def get_table_titles(page:int,file,df:pd.DataFrame) -> list():#pd.DataFrame:
    tbl_names_trunc = list()
    tbl_names = list(df[(df['page_number']== page) & (df['DataID_pdf']== file)]['final_table_title'])
    for tbl in tbl_names:
        if len(tbl) > 218:
            tbl_names_trunc.append(textwrap.shorten(tbl,width = 218))
        else:
            tbl_names_trunc.append(tbl)      
    return tbl_names_trunc
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


# In[4]:

def extract_tables(list_of_files, df_slice):
    for pdf_file in list_of_files:
        args.append([pdf_file, df_slice])
    
    result = []
    
    # Sequential execution mode:
    #for arg in args:
    #    result.append(extract_table(arg))
    
    # Multiprocessing execution mode    
    with multiprocessing.Pool() as pool:
        result = pool.map(extract_table, args)
    
#Main Function
def extract_table(argument_list):   
    file = argument_list[0]
    df = argument_list[1]
    
    failed_pdf = []
    start_time = datetime.now()

    file_path = 'F:/Environmental Baseline Data/Version 4 - Final/PDF/{}'.format(file)

    file_name = file_path.split('/')[-1].replace('.pdf','')

    try:
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
            #camelot table objects for each page of the pdf
            try:
                tables = camelot.read_pdf(file_path, pages = str(pg_num+1), flag_size=True, copy_text=['v'],strip_text = '\n',line_scale=40, f = 'csv',flavour = 'stream')  #loop len(tables)
            except:
                continue
            #get table names in page == pg_num by parsing get_table_titles() function
            title_lst_raw = get_table_titles(pg_num+1,file,df)
            title_lst = replace_chars_strings(title_lst_raw)
            #get total number of table objects detected by Camelot in page == pg_num
            tb_num = tables.n

            #VIEW
            print(title_lst)  
            #if Camelot returns NO table on the page continue the loop and go to the next page
            if tb_num == 0:
                print("No table on page "+ str(pg_num+1) + " is detected")
                continue
            #if whitespace of the detected table is larger than 69% of the entire table and there is only
            #one table on that page, identify this as figure and continute the loop and go to the next page
            elif tb_num == 1 and (tables[0].parsing_report)['whitespace'] > 69.0:
                print(f"Page {(tables[0].parsing_report)['page']} contains an image")  #add pdf ID!!!!
                continue
            #in case only one table is present on the page,
            elif tb_num == 1:
                #this block distills the dataframe with proper column names
                df_tb = tables[0].df
                df_tb = df_tb.replace('/na', '_', regex = True)
                colname = df_tb.iloc[0].str.replace('\n',' ',regex=True)
                df_tb.columns = colname
                df_tb = df_tb[1:]   
                #df_tb = df_tb.iloc[1:]
                #in case no title is extracted from this page but we know that there is one table
                if len(title_lst) == 0:
                    #let's say we are on page number x and this if statement checks whether page number x-1 contianed a table or not
                    #if the result is TRUE we assign the title of last table on previous page to this page's title-less table
                    if (pg_num-1 in title_dict):
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
                            xlsx_name = file_name + '_' + xl_name +'_'+str(pg_num+1)+'_'+str(1)+ '.csv'
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
        #                xl_name = xl_name.replace('/','_')
        #                xl_name = xl_name.replace(':','')
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
                        df_tb.columns = df_tb.iloc[0].str.replace('\n',' ',regex=True)
                        df_tb = df_tb[1:]
                        #df_tb = df_tb.iloc[1:]
                        xl_name = title_lst[j]
        #                    xl_name = xl_name.replace('/','_')
        #                    xl_name = xl_name.replace(':','')

                        #store page number, index of the table, and its name in a dictionary
                        chars.append([j,df_tb,xl_name])

                        xlsx_name = file_name + '_' + xl_name +'_'+str(pg_num+1)+'_'+str(j+1)+ '.csv'
                        df_tb.to_csv(xlsx_name, index = False, encoding='utf-8-sig')

                    title_dict[pg_num] = chars

                elif len(title_lst) < tb_num :
                    try:
                        #first case: if the first table in the page is conitnuos of what was on the previous page
                        df_tb = tables[0].df
                        df_tb = df_tb.replace('/na', '_', regex = True)
                        df_tb.columns = df_tb.iloc[0].str.replace('\n',' ',regex=True)
                        df_tb = df_tb[1:]
                        #df_tb = df_tb.iloc[1:]

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

                        indx = 0
                        for j in range(1,len(title_lst)+1):
                            df_tb = tables[j].df
                            df_tb = df_tb.replace('/na', '_', regex = True)
                            df_tb.columns = df_tb.iloc[0].str.replace('\n',' ',regex=True)
                            df_tb = df_tb[1:]
                            #df_tb = df_tb.iloc[1:]
                            xl_name = title_lst[indx]
                            indx = indx + 1
        #                        xl_name = xl_name.replace('/','_')
        #                        xl_name = xl_name.replace(':','')
                            #store page number, index of the table, and its name in a dictionary
                            chars.append([j,df_tb,xl_name])

                            xlsx_name = file_name + '_' + xl_name +'_'+str(pg_num+1)+'_'+str(j+1)+ '.csv'
                            #xlsx_name = z.split('\\')[-1] + '-' + str(pg[i]+1) + str(j) + '.xlsx'
                            df_tb.to_csv(xlsx_name, index = False, encoding='utf-8-sig')

                        for j in range(len(title_lst)+1,tb_num):
                            df_tb = tables[j].df
                            df_tb = df_tb.replace('/na', '_', regex = True)
                            df_tb.columns = df_tb.iloc[0].str.replace('\n',' ',regex=True)
                            df_tb = df_tb[1:]
                            #df_tb = df_tb.iloc[1:]
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
    except:
        failed_pdf.append(file)
        print("file {} failed to open".format(file))
        pass
        
    end_time = datetime.now()
    print('Duration: {}'.format(end_time - start_time)) 
    
    return failed_pdf


# In[8]:


path = 'F:/Environmental Baseline Data/Version 4 - Final/Support files/Table titles raw data/final_table_titles3.csv'
df = pd.read_csv(path, usecols = ['page_number','final_table_title', 'Application title short', 'DataID_pdf',                                   'categories', 'Category'])
df = df[df['categories'] > 0] 
df = df[df['Category'] == 'Table']
df['final_table_title'] = df['final_table_title'].str.title()
df.head()


df['Application title short'].unique().tolist()

#################################################################################################    


if __name__ == "__main__":
    hearing = 'Application for 2021 NGTL System Expansion Project' 
    ngtl2021 = df[df['Application title short'] == hearing].reset_index(drop = True)
    ngtl2021.head()

    os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\ngtl2021')

    files = list(ngtl2021['DataID_pdf'].unique())
    
    extract_tables(files,ngtl2021)
    
#################################################################################################    


hearing = 'Application for the 2017 NGTL System Expansion'
ngtl2017 = df[df['Application title short'] == hearing].reset_index(drop = True)
ngtl2017.head()


# ### Change this folder to the path were you want the tables saved

# In[ ]:


os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\ngtl2017')


# ### Change the dataframe name accordingly

# In[ ]:


files = list(ngtl2017['DataID_pdf'].unique())
files


# ### call the main function -- pass filename and dataframe as function arguments

# In[ ]:


extract_tables(files,ngtl2017)


# In[ ]:





# ## Application for Northwest Mainline Komie North Extension

# In[7]:


hearing = 'Application for Northwest Mainline Komie North Extension'
komie_north = df[df['Application title short'] == hearing].reset_index(drop = True)
komie_north.head()


# ### Change this folder to the path were you want the tables saved

# In[8]:


os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV2\komie_north2')


# ### Change the dataframe name accordingly

# In[9]:


files = list(komie_north['DataID_pdf'].unique())
files


# ### call the main function -- pass filename and dataframe as function arguments

# In[ ]:


extract_tables(files,komie_north)


# ## Application for the Edmonton to Hardisty Pipeline Project

# In[13]:


hearing = 'Application for the Edmonton to Hardisty Pipeline Project'
edmonton_hardisty = df[df['Application title short'] == hearing].reset_index(drop = True)
edmonton_hardisty.head()


# ### Change this folder to the path were you want the tables saved

# In[14]:


os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\edmonton_hardisty')


# ### Change the dataframe name accordingly

# In[15]:


files = list(edmonton_hardisty['DataID_pdf'].unique())
files


# ### call the main function -- pass filename and dataframe as function arguments

# In[ ]:


extract_tables(files, edmonton_hardisty)


# ## Application for Line 9 Reversal Phase I Project

# In[13]:


hearing = 'Application for Line 9 Reversal Phase I Project'
line9 = df[df['Application title short'] == hearing].reset_index(drop = True)
line9.head()


# ### Change this folder to the path were you want the tables saved

# In[14]:


os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\line9')


# ### Change the dataframe name accordingly

# In[16]:


files = list(line9['DataID_pdf'].unique())
extract_tables(files, line9)


# In[ ]:





# In[ ]:





# In[ ]:





# ## Application for the Leismer to Kettle River Crossover

# In[17]:


hearing = 'Application for the Leismer to Kettle River Crossover'
leismer2kettle = df[df['Application title short'] == hearing].reset_index(drop = True)
leismer2kettle.head()
# Change this folder to the path were you want the tables saved
os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV2\leismer2kettle2')
# Change the dataframe name accordingly
files = list(leismer2kettle['DataID_pdf'].unique())
extract_tables(files, leismer2kettle)


# ## Application for the Groundbirch Pipeline Project

# In[18]:


hearing = 'Application for the Groundbirch Pipeline Project'
groundbirch = df[df['Application title short'] == hearing].reset_index(drop = True)
groundbirch.head()
# Change this folder to the path were you want the tables saved
os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV2\groundbirch2')
# Change the dataframe name accordingly
files = list(groundbirch['DataID_pdf'].unique())
print(files)
extract_tables(files, groundbirch)


# In[ ]:





# In[19]:


hearing = 'Application for the Enbridge Northern Gateway Pipeline Project'
gateway = df[df['Application title short'] == hearing].reset_index(drop = True)
gateway.head()
# Change this folder to the path were you want the tables saved
os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\gateway')
# Change the dataframe name accordingly
files = list(gateway['DataID_pdf'].unique())
extract_tables(files, gateway)


# In[ ]:





# In[20]:


hearing = 'Application for the Horn River Project'
horn_river = df[df['Application title short'] == hearing].reset_index(drop = True)
horn_river.head()
# Change this folder to the path were you want the tables saved
os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\horn_river')
# Change the dataframe name accordingly
files = list(horn_river['DataID_pdf'].unique())
print(files)
extract_tables(files, horn_river)


# In[ ]:





# In[21]:


hearing = 'Application for Bakken Pipeline Project Canada'
bakken = df[df['Application title short'] == hearing].reset_index(drop = True)
bakken.head()
# Change this folder to the path were you want the tables saved
os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\bakken')
# Change the dataframe name accordingly
files = list(bakken['DataID_pdf'].unique())
print(files)
extract_tables(files, bakken)


# In[ ]:





# In[22]:


hearing = 'Application for the Vantage Pipeline Project'
vantage = df[df['Application title short'] == hearing].reset_index(drop = True)
vantage.head()
# Change this folder to the path were you want the tables saved
os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\vantage')
# Change the dataframe name accordingly
files = list(vantage['DataID_pdf'].unique())
extract_tables(files, vantage)    


# In[ ]:





# In[23]:


hearing = 'Application for the Northwest Mainline Expansion'
northwest_mainline = df[df['Application title short'] == hearing].reset_index(drop = True)
northwest_mainline.head()
# Change this folder to the path were you want the tables saved
os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\northwest_mainline')
# Change the dataframe name accordingly
files = list(northwest_mainline['DataID_pdf'].unique())
print(files)
extract_tables(files, northwest_mainline)  


# In[ ]:





# In[24]:


hearing = 'Application for the Line 4 Extension Project'
line4 = df[df['Application title short'] == hearing].reset_index(drop = True)
line4.head()
# Change this folder to the path were you want the tables saved
os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\line4')
# Change the dataframe name accordingly
files = list(line4['DataID_pdf'].unique())
extract_tables(files, line4) 


# In[ ]:





# In[26]:


hearing = 'Application for the Cushing Expansion'
cushing_expansion = df[df['Application title short'] == hearing].reset_index(drop = True)
cushing_expansion.head()
# Change this folder to the path were you want the tables saved
os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\cushing_expansion')
# Change the dataframe name accordingly
files = list(cushing_expansion['DataID_pdf'].unique())
print(files)
extract_tables(files, cushing_expansion) 


# In[ ]:





# In[27]:


hearing = 'Application for Redwillow Pipeline Project'
red_willow = df[df['Application title short'] == hearing].reset_index(drop = True)
red_willow.head()
# Change this folder to the path were you want the tables saved
os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\red_willow')
# Change the dataframe name accordingly
files = list(red_willow['DataID_pdf'].unique())
print(files)
extract_tables(files, red_willow) 


# In[ ]:





# In[28]:


hearing = 'Application to construct and operate the South Peace Pipeline Project'
south_peace = df[df['Application title short'] == hearing].reset_index(drop = True)
south_peace.head()
# Change this folder to the path were you want the tables saved
os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\south_peace')
# Change the dataframe name accordingly
files = list(south_peace['DataID_pdf'].unique())
print(files)
extract_tables(files, south_peace) 


# In[ ]:





# In[29]:


hearing = 'Application for the Keystone XL Pipeline'
keystoneXL = df[df['Application title short'] == hearing].reset_index(drop = True)
keystoneXL.head()
# Change this folder to the path were you want the tables saved
os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\keystoneXL')
# Change the dataframe name accordingly
files = list(keystoneXL['DataID_pdf'].unique())
print(files)
extract_tables(files, keystoneXL) 


# In[ ]:





# In[30]:


hearing = 'Application for the Alberta Clipper Expansion Project'
alberta_clipper = df[df['Application title short'] == hearing].reset_index(drop = True)
alberta_clipper.head()
# Change this folder to the path were you want the tables saved
os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\alberta_clipper')
# Change the dataframe name accordingly
files = list(alberta_clipper['DataID_pdf'].unique())
print(files)
extract_tables(files, alberta_clipper) 


# In[ ]:





# In[31]:


hearing = 'Application for Line 13 Transfer, Line 13 Reversal and Capacity Replacement for the Southern Lights Project'
southern_lights = df[df['Application title short'] == hearing].reset_index(drop = True)
southern_lights.head()
# Change this folder to the path were you want the tables saved
os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\southern_lights')
# Change the dataframe name accordingly
files = list(southern_lights['DataID_pdf'].unique())
print(files)
extract_tables(files, southern_lights) 


# In[ ]:





# In[32]:


hearing = 'Application for the Brunswick Pipeline Project'
brunswick = df[df['Application title short'] == hearing].reset_index(drop = True)
brunswick.head()
# Change this folder to the path were you want the tables saved
os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\brunswick')
# Change the dataframe name accordingly
files = list(brunswick['DataID_pdf'].unique())
print(files)
extract_tables(files, brunswick) 


# In[ ]:





# In[ ]:


hearing = 'Application for the Construction of North Corridor Expansion Project'
north_corridor = df[df['Application title short'] == hearing].reset_index(drop = True)
north_corridor.head()
# Change this folder to the path were you want the tables saved
os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\north_corridor')
# Change the dataframe name accordingly
files = list(north_corridor['DataID_pdf'].unique())
print(files)
extract_tables(files, north_corridor) 


# In[33]:


hearing = 'Application for the Construction of Edson Mainline Expansion Project'
edson_mainline = df[df['Application title short'] == hearing].reset_index(drop = True)
edson_mainline.head()
# Change this folder to the path were you want the tables saved
os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\edson_mainline')
# Change the dataframe name accordingly
files = list(edson_mainline['DataID_pdf'].unique())
print(files)
extract_tables(files, edson_mainline) 


# In[ ]:





# In[34]:


hearing = 'Application for Leave to Abandon Deep Panuke Pipeline'
deep_panuke = df[df['Application title short'] == hearing].reset_index(drop = True)
deep_panuke.head()
# Change this folder to the path were you want the tables saved
os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\deep_panuke')
# Change the dataframe name accordingly
files = list(deep_panuke['DataID_pdf'].unique())
print(files)
extract_tables(files, deep_panuke) 


# In[ ]:





# In[35]:


hearing = 'Application for the Goldboro Gas Plant and 26" Gathering Pipeline Abandonment'
goldboro_abandonment = df[df['Application title short'] == hearing].reset_index(drop = True)
goldboro_abandonment.head()
# Change this folder to the path were you want the tables saved
os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\goldboro_abandonment')
# Change the dataframe name accordingly
files = list(goldboro_abandonment['DataID_pdf'].unique())
print(files)
extract_tables(files, goldboro_abandonment) 


# In[ ]:





# In[36]:


hearing = 'Application for the construction of the West Path Delivery Project'
west_path = df[df['Application title short'] == hearing].reset_index(drop = True)
west_path.head()
# Change this folder to the path were you want the tables saved
os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\west_path')
# Change the dataframe name accordingly
files = list(west_path['DataID_pdf'].unique())
print(files)
extract_tables(files, west_path) 


# In[ ]:





# In[37]:


hearing = 'Application for the Spruce Ridge Program'
spruce_ridge = df[df['Application title short'] == hearing].reset_index(drop = True)
spruce_ridge.head()
# Change this folder to the path were you want the tables saved
os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\spruce_ridge')
# Change the dataframe name accordingly
files = list(spruce_ridge['DataID_pdf'].unique())
print(files)
extract_tables(files, spruce_ridge) 


# In[ ]:





# In[38]:


hearing = 'Application for the Spruce Ridge Program'
spruce_ridge = df[df['Application title short'] == hearing].reset_index(drop = True)
spruce_ridge.head()
# Change this folder to the path were you want the tables saved
os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\spruce_ridge')
# Change the dataframe name accordingly
files = list(spruce_ridge['DataID_pdf'].unique())
print(files)
extract_tables(files, spruce_ridge) 


# In[ ]:





# In[39]:
# Number 1

hearing = 'Applications for Energy East, Asset Transfer and Eastern Mainline'
energy_east = df[df['Application title short'] == hearing].reset_index(drop = True)
energy_east.head()
# Change this folder to the path were you want the tables saved
os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV2\energy_east2')
# Change the dataframe name accordingly
files = list(energy_east['DataID_pdf'].unique())
print(files)

files =['2968357.pdf', '2968358.pdf', '2968365.pdf', '2968366.pdf', '2968367.pdf', '2968368.pdf', '2968370.pdf', '2968373.pdf', '2968377.pdf', '2968378.pdf', '2968379.pdf', '2968380.pdf', '2968417.pdf', '2968421.pdf', '2968422.pdf', '2968425.pdf', '2968426.pdf', '2968427.pdf', '2968430.pdf', '2968431.pdf', '2968432.pdf', '2968434.pdf', '2968436.pdf', '2968440.pdf', '2968448.pdf', '2968450.pdf', '2968452.pdf', '2968457.pdf', '2968458.pdf', '2968462.pdf', '2968464.pdf', '2968467.pdf', '2968468.pdf', '2968470.pdf', '2968475.pdf', '2968476.pdf', '2968477.pdf', '2968478.pdf', '2968479.pdf', '2968480.pdf', '2969255.pdf', '2969256.pdf', '2969257.pdf', '2969269.pdf', '2969272.pdf', '2969283.pdf', '2969358.pdf', '2969359.pdf', '2969375.pdf', '2969376.pdf', '2969377.pdf', '2969378.pdf', '2969379.pdf', '2969456.pdf', '2969457.pdf', '2969460.pdf', '2969462.pdf', '2969464.pdf', '2969466.pdf', '2969469.pdf', '2969470.pdf', '2969473.pdf', '2969475.pdf', '2969476.pdf', '2969477.pdf', '2969479.pdf', '2969480.pdf', '2969481.pdf', '2969496.pdf', '2969497.pdf', '2969557.pdf', '2969564.pdf', '2969567.pdf', '2969569.pdf', '2969570.pdf', '2969571.pdf', '2969586.pdf', '2969587.pdf', '2969659.pdf', '2969663.pdf', '2969665.pdf', '2969666.pdf', '2969674.pdf', '2969675.pdf', '2969678.pdf', '2969679.pdf', '2969680.pdf', '2969688.pdf', '2969689.pdf', '2969759.pdf', '2969763.pdf', '2969765.pdf', '2969766.pdf', '2969767.pdf', '2969780.pdf', '2969781.pdf', '2969782.pdf', '2969783.pdf', '2969865.pdf', '2969866.pdf', '2969880.pdf', '2969881.pdf', '2969882.pdf', '2969968.pdf', '2969969.pdf', '2969970.pdf']
files = ['2968356_fixed.pdf']
extract_tables(files, energy_east) 

# extract_tables('2968356_fixed.pdf', energy_east) 

# In[ ]:





# In[43]:


hearing = 'Application for the Wyndwood Pipeline Expansion Project'
wyndwood = df[df['Application title short'] == hearing].reset_index(drop = True)
wyndwood.head()
# Change this folder to the path were you want the tables saved
os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\wyndwood')
# Change the dataframe name accordingly
files = list(wyndwood['DataID_pdf'].unique())
print(files)
extract_tables(files, wyndwood) 


# In[ ]:





# In[45]:


hearing = 'Application for North Montney Project'
north_montney = df[df['Application title short'] == hearing].reset_index(drop = True)
north_montney.head()
# Change this folder to the path were you want the tables saved
os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\north_montney')
# Change the dataframe name accordingly
files = list(north_montney['DataID_pdf'].unique())
print(files)
extract_tables(files, north_montney) 


# In[ ]:





# In[46]:


hearing = 'Application for Trans Mountain Expansion Project'
trans_mountain = df[df['Application title short'] == hearing].reset_index(drop = True)
trans_mountain.head()
# Change this folder to the path were you want the tables saved
os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\trans_mountain')
# Change the dataframe name accordingly
files = list(trans_mountain['DataID_pdf'].unique())
print(files)
extract_tables(files, trans_mountain) 


# In[ ]:





# In[47]:


hearing = 'Application for the Wolverine River Lateral Loop Carmon Creek Section'
carmon_creek = df[df['Application title short'] == hearing].reset_index(drop = True)
carmon_creek.head()
# Change this folder to the path were you want the tables saved
os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\carmon_creek')
# Change the dataframe name accordingly
files = list(carmon_creek['DataID_pdf'].unique())
print(files)
extract_tables(files, carmon_creek) 


# In[ ]:





# In[48]:


hearing = 'Applications for Energy East, Asset Transfer and Eastern Mainline  Eastern Mainline ESA'
eastern_mainline = df[df['Application title short'] == hearing].reset_index(drop = True)
eastern_mainline.head()
# Change this folder to the path were you want the tables saved
os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\eastern_mainline')
# Change the dataframe name accordingly
files = list(eastern_mainline['DataID_pdf'].unique())
print(files)
extract_tables(files, eastern_mainline) 


# ### Number 7 - Sousan's

# In[ ]:


hearing = 'Application for the Line 3 Replacement Program'
line3 = df[df['Application title short'] == hearing].reset_index(drop = True)
line3.head()
# Change this folder to the path were you want the tables saved
os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV2\line3_2')
# Change the dataframe name accordingly
files = list(line3['DataID_pdf'].unique())
print(files)
extract_tables(files, line3)


# ### Number 8 - Sousan's

# In[14]:


hearing = 'Application to Construct and Operate Ekwan Pipeline'
ekwan = df[df['Application title short'] == hearing].reset_index(drop = True)
ekwan.head()
# Change this folder to the path were you want the tables saved
os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV2\ekwan2')
# Change the dataframe name accordingly
files = list(ekwan['DataID_pdf'].unique())
print(files)
extract_tables(files, ekwan) 


# ### Number 9 - Sousan's

# In[13]:


hearing = 'Application for the Towerbirch Expansion Project'
towerbirch = df[df['Application title short'] == hearing].reset_index(drop = True)
towerbirch.head()
# Change this folder to the path were you want the tables saved
os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV2\towerbirch2')
# Change the dataframe name accordingly
files = list(towerbirch['DataID_pdf'].unique())
print(files)
extract_tables(files, towerbirch) 


# ## Number 10 - Sousan's

# In[11]:


hearing = 'Application for the Albersun Pipeline Asset Purchase G'
albersun = df[df['Application title short'] == hearing].reset_index(drop = True)
albersun.head()
# Change this folder to the path were you want the tables saved
os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV2\albersun2')
# Change the dataframe name accordingly
files = list(albersun['DataID_pdf'].unique())
print(files)


# In[12]:


extract_tables(files, albersun) 


# In[ ]:




