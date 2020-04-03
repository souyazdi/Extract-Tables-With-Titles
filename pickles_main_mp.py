# -*- coding: utf-8 -*-
"""
Created on Mon Mar  2 08:59:30 2020

@author: T1Sousan
"""

#Libraies
from io import StringIO
import time
from bs4 import BeautifulSoup
from tika import parser
import pandas as pd
from collections import Counter
import pandas as pd
import sys
import multiprocessing
sys.path.insert(0, 'H:/GitHub/Extract-Tables-With-Titles')
import pickles_functions_mp as pf
import glob
#
if __name__ == "__main__":
    #Get unique project names
    #Open Index 2 for each PDF
#    #updated on Feb 27 post line 3 addition
#    index2_path = 'F:/Environmental Baseline Data/Version 4 - Final/Indices/Index 2 - PDFs for Major Projects with ESAs.csv'
#    index2 = pd.read_csv(index2_path)
#    index2['Application title short'].unique()
#
#    subset_list_pdf = list(index2['DataID_pdf'])
    subset_list_pdf_full = ['F:/Environmental Baseline Data/Version 4 - Final/PDF/' + x.split('\\')[-1] for x in glob.glob('F:/Environmental Baseline Data/Version 4 - Final/PDF/*.pdf')]
    #a = get_argument(subset_list_pdf_full)

    starttime = time.time()
    processes = []
    for i in subset_list_pdf_full:
        try:
            p = multiprocessing.Process(target= pf.get_pickles_, args=(i,))
            processes.append(p)
            p.start()
        except:
            print("file {} is not present in the folder".format(i.split('/')[-1]))
            continue
    for process in processes:
        process.join()
    print('That took {} seconds'.format(time.time() - starttime))

     
    
if __name__ == "__main__":

    subset_list_pdf_full = ['F:/Environmental Baseline Data/Version 4 - Final/PDF/' + x.split('\\')[-1] for x in glob.glob('F:/Environmental Baseline Data/Version 4 - Final/PDF/*.pdf')]
    #a = get_argument(subset_list_pdf_full)

    starttime = time.time()
    processes = []

    for i in subset_list_pdf_full:
        try:
            pf.rotate_pdf(i)
        except Exception:
            print(f'{i} failed')
            pass
        
        
    print('That took {} seconds'.format(time.time() - starttime))
    
  
    
########################################################################################################################################

        
    
if __name__ == '__main__':
    # list of full paths to pdfs
    subset_list_pdf_full = ['F:/Environmental Baseline Data/Version 4 - Final/PDF/'
                            + x.split('\\')[-1] for x in glob.glob
                            ('F:/Environmental Baseline Data/Version 4 - Final/PDF/*.pdf')]
    subset_list_pdf_full = subset_list_pdf_full[0:40]
    # Directory where the output pickle files are saved
    path = 'H:/GitHub/tmp/'
    
    # prepare arguments for multiprocessing
    args = pf.get_argument(subset_list_pdf_full, path) 

    # timing the process-start
    starttime = time.time()
    
    # sequential
    for arg in args:
        try:
            pf.pickle_pdf_xml(arg)
        except Exception:
            print(f'{i} failed')
            pass

    # multiprocessing
    pool = multiprocessing.Pool()
   # pool.map(pf.rotate_pdf, subset_list_pdf_full)
    pool.map(pf.pickle_pdf_xml, args)
    pool.close()
    
    # time ends and dellta displayed
    print('That took {} seconds'.format(time.time() - starttime))  
    
    with multiprocessing.Pool() as pool:
        outputs = pool.map(mf.extract_tables_noname, args)