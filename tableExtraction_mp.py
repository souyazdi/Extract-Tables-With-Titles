#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import os
import sys
sys.path.insert(0, 'H:/GitHub/Extract-Tables-With-Titles')
import functions_mp as mf
import multiprocessing
import time
import glob
import random
from multiprocessing import Semaphore
import numpy as np

path1 = '//luxor/data/branch/Environmental Baseline Data/Version 4 - Final/Support files/Table titles raw data/final_table_titles8.csv'
path2 = '//luxor/data/branch/Environmental Baseline Data/Version 4 - Final/Support files/Table titles raw data/mackenzie_table_title.csv'
#df = pd.read_csv(path, usecols = ['page_number','final_table_title', 'Application title short', 'DataID_pdf','categories', 'Category'])
df1 = pd.read_csv(path1, usecols = ['page_number','final_table_title', 'DataID','categories', 'Category'])
df2 = pd.read_csv(path2, 'utf-8', engine = 'python',delimiter = ',')
df2 = df2[['page_number','final_table_title', 'DataID','categories', 'Category']]
frames = [df1,df2]
df = pd.concat(frames)
df['DataID'] = df['DataID'].astype(str)
df = df[df['categories'] > 0] 
df = df[df['Category'] == 'Table']
df['final_table_title'] = df['final_table_title'].str.title()
df.head()


#######################If running multiprocessing    
if __name__ == "__main__":
#    hearing = 'Application for 2021 NGTL System Expansion Project'
#    ngtl = df[df['Application title short'] == hearing].reset_index(drop = True)
#    ngtl.head()
    # Change this folder to the path were you want the tables saved
    os.chdir(r'H:\GitHub\tmp\mp_tbl_mp')
    # Change the dataframe name accordingly
    #files = list(ngtl['DataID_pdf'].unique())
    #files = [os.path.basename(x) for x in glob.glob(r'F:\Environmental Baseline Data\Version 4 - Final\PDF\*.pdf')]
    a = mf.extract_tables(files, df)
    starttime = time.time()
    processes = []
    for i in a:
        p = multiprocessing.Process(target= mf.extract_tables_noname, args=(i,))
        processes.append(p)
        p.start()
        
    for process in processes:
        process.join()
       
    print('That took {} seconds'.format(time.time() - starttime))
#################################################################################    

#if __name__ == "__main__":
#    # Change this folder to the path were you want the tables saved
#    subset_list_pdf_full = ['F:/Environmental Baseline Data/Version 4 - Final/PDF/' + x.split('\\')[-1] for x in glob.glob('F:/Environmental Baseline Data/Version 4 - Final/PDF/*.pdf')]
#
#    os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV2\tst')
#    # Change the dataframe name accordingly
#    files = subset_list_pdf_full[0:50]
#    a = mf.extract_tables(files, df)
#    
#    starttime = time.time()
#    processes = []
#    for i in a:
#        p = multiprocessing.Process(target= mf.extract_table, args=(i,))
#        processes.append(p)
#        p.start()
#        
#    for process in processes:
#        process.join()
#       
#    print('That took {} seconds'.format(time.time() - starttime))
##################################################################################################################      
if __name__ == '__main__':
    #files = [os.path.basename(x) for x in glob.glob(r'F:\Environmental Baseline Data\Version 4 - Final\PDF\*.pdf')]
    path = '//luxor/data/branch/Environmental Baseline Data/Version 4 - Final/CSV_final_JSON/'
    path_ = '//luxor/data/branch/Environmental Baseline Data/Version 4 - Final/PDF/'
    lst_json = [(fname.split('\\')[-1]).split('_')[0] for fname in glob.glob(path+'*.txt')]
    lst_pdf = [(fname.split('\\')[-1]).replace('.pdf','') for fname in glob.glob(path_+'*.pdf')]
    files = [item+'.pdf' for item in lst_pdf if item not in lst_json]        
        
    args = mf.create_arguments(files, df) 
    starttime = time.time()
    outputs = []
#       
#    #******************************************************
#    for arg in args:
#        outputs.append(mf.extract_tables_noname(arg))

#    #******************************************************
    with multiprocessing.Pool() as pool:
        outputs = pool.map(mf.extract_tables_noname, args)
    
    print('That took {} seconds'.format(time.time() - starttime))      


#learn starmap for multiprocessing     
#################################################################################    
    
#if __name__ == '__main__':
#    hearing = 'Application for the Brunswick Pipeline Project'
#    brunswick = df[df['Application title short'] == hearing].reset_index(drop = True)
#    brunswick.head()
#    # Change this folder to the path were you want the tables saved
#    os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV - Copy\brunswick_')
#    # Change the dataframe name accordingly
#    files = list(brunswick['DataID_pdf'].unique())
#    print(files)
#    
#    a = mf.extract_tables(files, brunswick)
#    starttime = time.time()
#    pool = multiprocessing.Pool()
#    pool.map(mf.extract_table, a)
#    pool.close()
#    print('That took {} seconds'.format(time.time() - starttime))  
##    
##    hearing = 'Application for the Edmonton to Hardisty Pipeline Project'
##    edmonton_hardisty = df[df['Application title short'] == hearing].reset_index(drop = True)
##    os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV - Copy\edmonton_hardisty')
##    files = list(edmonton_hardisty['DataID_pdf'].unique())
##    
##    a = extract_tables(files, edmonton_hardisty)
##    print(a)
##    starttime = time.time()
##    pool = multiprocessing.Pool()
##    pool.map(mf.extract_table, a)
##    pool.close()
##    print('That took {} seconds'.format(time.time() - starttime))  
##    
#     
########################If running sequential
#hearing = 'Application for 2021 NGTL System Expansion Project' 
#ngtl2021 = df[df['Application title short'] == hearing].reset_index(drop = True)
#os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV - Copy\ngtl2021')
#files = list(ngtl2021['DataID_pdf'].unique())
#for file in files:
#    mf.extract_table_(file,ngtl2021)     
#    
#    
#tbls_dict = dict()
#hearing = 'Application for 2021 NGTL System Expansion Project' 
#ngtl2021 = df[df['Application title short'] == hearing].reset_index(drop = True)
#os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV - Copy\ngtl2021')
#files = list(ngtl2021['DataID_pdf'].unique())
#for file in files:
#    file_path = 'F:/Environmental Baseline Data/Version 4 - Final/PDF/{}'.format(file)
#    tables = camelot.read_pdf(file_path, pages = 'all', flag_size=True, copy_text=['v'],strip_text = '\n',line_scale=40, f = 'csv',flavour = 'stream')  #loop len(tables)
#    tbls_dict[file] = tables.n
#    
#    
    
#hearing = 'Application for 2021 NGTL System Expansion Project' 
#ngtl2021 = df[df['Application title short'] == hearing].reset_index(drop = True)
#os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\ngtl2021')
#files = list(ngtl2021['DataID_pdf'].unique())
#extract_tables(file,ngtl2021)
#
#hearing = 'Application for the 2017 NGTL System Expansion'
#ngtl2017 = df[df['Application title short'] == hearing].reset_index(drop = True)
#os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\ngtl2017')
#files = list(ngtl2017['DataID_pdf'].unique())
#extract_tables(file,ngtl2017)
#
#hearing = 'Application for Northwest Mainline Komie North Extension'
#komie_north = df[df['Application title short'] == hearing].reset_index(drop = True)
#os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\komie_north')
#files = list(komie_north['DataID_pdf'].unique())
#extract_tables(file,komie_north)
#    
#hearing = 'Application for the Edmonton to Hardisty Pipeline Project'
#edmonton_hardisty = df[df['Application title short'] == hearing].reset_index(drop = True)
#os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\edmonton_hardisty')
#files = list(edmonton_hardisty['DataID_pdf'].unique())
#extract_tables(file, edmonton_hardisty)
##
#hearing = 'Application for Line 9 Reversal Phase I Project'
#line9 = df[df['Application title short'] == hearing].reset_index(drop = True)
#os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\line9')
#files = list(line9['DataID_pdf'].unique())
#extract_tables(file, line9)
##
#hearing = 'Application for the Leismer to Kettle River Crossover'
#leismer2kettle = df[df['Application title short'] == hearing].reset_index(drop = True)
#os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\leismer2kettle')
#files = list(leismer2kettle['DataID_pdf'].unique())
#extract_tables(file, leismer2kettle)
#
#hearing = 'Application for the Groundbirch Pipeline Project'
#groundbirch = df[df['Application title short'] == hearing].reset_index(drop = True)
#os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\groundbirch')
#files = list(groundbirch['DataID_pdf'].unique())
#extract_tables(file, groundbirch)
#
#hearing = 'Application for the Enbridge Northern Gateway Pipeline Project'
#gateway = df[df['Application title short'] == hearing].reset_index(drop = True)
#os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\gateway')
#files = list(gateway['DataID_pdf'].unique())
#extract_tables(file, gateway)
#
#hearing = 'Application for the Horn River Project'
#horn_river = df[df['Application title short'] == hearing].reset_index(drop = True)
#os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV2\horn_river')
#files = list(horn_river['DataID_pdf'].unique())
#extract_tables(file, horn_river)
#
#hearing = 'Application for Bakken Pipeline Project Canada'
#bakken = df[df['Application title short'] == hearing].reset_index(drop = True)
#os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\bakken')
#files = list(bakken['DataID_pdf'].unique())
#extract_tables(file, bakken)
#
#hearing = 'Application for the Vantage Pipeline Project'
#vantage = df[df['Application title short'] == hearing].reset_index(drop = True)
#os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\vantage')
#files = list(vantage['DataID_pdf'].unique())
#extract_tables(file, vantage)    
#
#hearing = 'Application for the Northwest Mainline Expansion'
#northwest_mainline = df[df['Application title short'] == hearing].reset_index(drop = True)
#os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\northwest_mainline')
#files = list(northwest_mainline['DataID_pdf'].unique())
#extract_tables(file, northwest_mainline)  
#
#hearing = 'Application for the Line 4 Extension Project'
#line4 = df[df['Application title short'] == hearing].reset_index(drop = True)
#os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\line4')
#files = list(line4['DataID_pdf'].unique())
##extract_tables(file, line4) 
##
#hearing = 'Application for the Cushing Expansion'
#cushing_expansion = df[df['Application title short'] == hearing].reset_index(drop = True)
#os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\cushing_expansion')
#files = list(cushing_expansion['DataID_pdf'].unique())
#extract_tables(file, cushing_expansion) 
#
#hearing = 'Application for Redwillow Pipeline Project'
#red_willow = df[df['Application title short'] == hearing].reset_index(drop = True)
#os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\red_willow')
#files = list(red_willow['DataID_pdf'].unique())
#extract_tables(file, red_willow) 
#
#hearing = 'Application to construct and operate the South Peace Pipeline Project'
#south_peace = df[df['Application title short'] == hearing].reset_index(drop = True)
#os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\south_peace')
#files = list(south_peace['DataID_pdf'].unique())
#extract_tables(file, south_peace) 
#
#hearing = 'Application for the Keystone XL Pipeline'
#keystoneXL = df[df['Application title short'] == hearing].reset_index(drop = True)
#os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\keystoneXL')
#files = list(keystoneXL['DataID_pdf'].unique())
#extract_tables(file, keystoneXL) 
#
#hearing = 'Application for the Alberta Clipper Expansion Project'
#alberta_clipper = df[df['Application title short'] == hearing].reset_index(drop = True)
#os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\alberta_clipper')
#files = list(alberta_clipper['DataID_pdf'].unique())
#extract_tables(file, alberta_clipper) 
#
#hearing = 'Application for Line 13 Transfer, Line 13 Reversal and Capacity Replacement for the Southern Lights Project'
#southern_lights = df[df['Application title short'] == hearing].reset_index(drop = True)
#os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\southern_lights')
#files = list(southern_lights['DataID_pdf'].unique())
#extract_tables(file, southern_lights) 
##
#hearing = 'Application for the Brunswick Pipeline Project'
#brunswick = df[df['Application title short'] == hearing].reset_index(drop = True)
#os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV - Copy\brunswick_')
#files = list(brunswick['DataID_pdf'].unique())
#for file in files:
#    mf.extract_table_(file, brunswick) 
#
#hearing = 'Application for the Construction of North Corridor Expansion Project'
#north_corridor = df[df['Application title short'] == hearing].reset_index(drop = True)
#os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\north_corridor')
#files = list(north_corridor['DataID_pdf'].unique())
#extract_tables(file, north_corridor) 
#
#hearing = 'Application for the Construction of Edson Mainline Expansion Project'
#edson_mainline = df[df['Application title short'] == hearing].reset_index(drop = True)
#os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\edson_mainline')
#files = list(edson_mainline['DataID_pdf'].unique())
#extract_tables(file, edson_mainline) 
#
#hearing = 'Application for Leave to Abandon Deep Panuke Pipeline'
#deep_panuke = df[df['Application title short'] == hearing].reset_index(drop = True)
#os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\deep_panuke')
#files = list(deep_panuke['DataID_pdf'].unique())
#extract_tables(file, deep_panuke) 
#
#hearing = 'Application for the Goldboro Gas Plant and 26" Gathering Pipeline Abandonment'
#goldboro_abandonment = df[df['Application title short'] == hearing].reset_index(drop = True)
#os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\goldboro_abandonment')
#files = list(goldboro_abandonment['DataID_pdf'].unique())
#extract_tables(file, goldboro_abandonment) 
#
#hearing = 'Application for the construction of the West Path Delivery Project'
#west_path = df[df['Application title short'] == hearing].reset_index(drop = True)
#os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\west_path')
#files = list(west_path['DataID_pdf'].unique())
#extract_tables(file, west_path) 
#
#hearing = 'Application for the Spruce Ridge Program'
#spruce_ridge = df[df['Application title short'] == hearing].reset_index(drop = True)
#os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\spruce_ridge')
#files = list(spruce_ridge['DataID_pdf'].unique())
#extract_tables(file, spruce_ridge) 
#
#hearing = 'Application for the Spruce Ridge Program'
#spruce_ridge = df[df['Application title short'] == hearing].reset_index(drop = True)
#os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV2\spruce_ridge')
#files = list(spruce_ridge['DataID_pdf'].unique())
#extract_tables(file, spruce_ridge) 
#
#hearing = 'Applications for Energy East, Asset Transfer and Eastern Mainline'
#energy_east = df[df['Application title short'] == hearing].reset_index(drop = True)
#os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV2\energy_east')
#files = list(energy_east['DataID_pdf'].unique())
#extract_tables(file, energy_east) 
#
#hearing = 'Application for the Wyndwood Pipeline Expansion Project'
#wyndwood = df[df['Application title short'] == hearing].reset_index(drop = True)
#os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV2\wyndwood')
#files = list(wyndwood['DataID_pdf'].unique())
#extract_tables(file, wyndwood) 
#
#hearing = 'Application for North Montney Project'
#north_montney = df[df['Application title short'] == hearing].reset_index(drop = True)
#os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV2\north_montney')
#files = list(north_montney['DataID_pdf'].unique())
#extract_tables(file, north_montney) 
#
#hearing = 'Application for Trans Mountain Expansion Project'
#trans_mountain = df[df['Application title short'] == hearing].reset_index(drop = True)
#os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV\trans_mountain')
#files = list(trans_mountain['DataID_pdf'].unique())
#extract_tables(file, trans_mountain) 
#
#hearing = 'Application for the Wolverine River Lateral Loop Carmon Creek Section'
#carmon_creek = df[df['Application title short'] == hearing].reset_index(drop = True)
#os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV2\carmon_creek')
#files = list(carmon_creek['DataID_pdf'].unique())
#extract_tables(file, carmon_creek) 
#
#hearing = 'Applications for Energy East, Asset Transfer and Eastern Mainline  Eastern Mainline ESA'
#eastern_mainline = df[df['Application title short'] == hearing].reset_index(drop = True)
#os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV2\eastern_mainline')
#files = list(eastern_mainline['DataID_pdf'].unique())
#extract_tables(file, eastern_mainline) 
#
#hearing = 'Application for the Line 3 Replacement Program'
#line3 = df[df['Application title short'] == hearing].reset_index(drop = True)
#os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV2\line3')
#files = list(line3['DataID_pdf'].unique())
#extract_tables(file, line3)
#
#hearing = 'Application to Construct and Operate Ekwan Pipeline'
#ekwan = df[df['Application title short'] == hearing].reset_index(drop = True)
#os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV2\ekwan')
#files = list(ekwan['DataID_pdf'].unique())
#extract_tables(file, ekwan) 
#
#hearing = 'Application for the Towerbirch Expansion Project'
#towerbirch = df[df['Application title short'] == hearing].reset_index(drop = True)
#os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV2\towerbirch')
#files = list(towerbirch['DataID_pdf'].unique())
#extract_tables(file, towerbirch) 
#
#hearing = 'Application for the Albersun Pipeline Asset Purchase G'
#albersun = df[df['Application title short'] == hearing].reset_index(drop = True)
#os.chdir(r'F:\Environmental Baseline Data\Version 4 - Final\CSV2\albersun')
#files = list(albersun['DataID_pdf'].unique())
#extract_tables(file, albersun) 
#
#
#
