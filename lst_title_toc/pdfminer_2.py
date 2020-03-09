# -*- coding: utf-8 -*-
"""
Created on Thu Mar  5 10:00:37 2020

@author: yazdsous
"""

from pdfminer.pdfparser import PDFParser
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfpage import PDFTextExtractionNotAllowed
from pdfminer.pdfinterp import PDFResourceManager
from pdfminer.pdfinterp import PDFPageInterpreter
from pdfminer.pdfdevice import PDFDevice
from pdfminer.layout import LAParams, LTTextBox
from pdfminer.converter import PDFPageAggregator
import pdfminer


file = 'A6T2W6.pdf'
file = 'A6T2G9.pdf'
file_path = 'F:/Environmental Baseline Data/Version 3/Data/PDF/{}'.format(file)


fp = open(file_path, 'rb')
rsrcmgr = PDFResourceManager()
laparams = LAParams()
device = PDFPageAggregator(rsrcmgr, laparams=laparams)
interpreter = PDFPageInterpreter(rsrcmgr, device)
pages = PDFPage.get_pages(fp)

for page in pages:
    print('Processing next page...')
    interpreter.process_page(page)
    layout = device.get_result()
    for lobj in layout:
        if isinstance(lobj, LTTextBox):
            x, y, text = lobj.bbox[0], lobj.bbox[3], lobj.get_text()
            print('At %r is text: %s' % ((x, y), text))
       
        elif isinstance(lobj, LTFigure):
            x, y, text = lobj.bbox[0], lobj.bbox[3], lobj.get_text()
            print('At %r is text: %s' % ((x, y), text))
        
        elif isinstance(lobj, LTImage):
            x, y, text = lobj.bbox[0], lobj.bbox[3], lobj.get_text()
            print('At %r is text: %s' % ((x, y), text))
            
        elif isinstance(lobj, LTLine):
            x, y, text = lobj.bbox[0], lobj.bbox[3], lobj.get_text()
            print('At %r is text: %s' % ((x, y), text))
            
       elif isinstance(lobj, LTRect):
            x, y, text = lobj.bbox[0], lobj.bbox[3], lobj.get_text()
            print('At %r is text: %s' % ((x, y), text))
            
            
            
            
