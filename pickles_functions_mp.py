# -*- coding: utf-8 -*-
"""
Created on Mon Mar  2 09:44:41 2020

@author: T1Sousan
"""

from tika import parser
import pickle
import PyPDF2


##create the argument
def get_argument(list_of_files, path):
    args = []
    for pdf_file in list_of_files:
        args.append([pdf_file, path])
    return args


def pickle_pdf_xml(arguments):
    file = arguments[0]
    path = arguments[1]
    xml = parser.from_file(file, xmlContent = True)
    replace_string = file.split('/')[-1].replace('.pdf', '')
    save_string = path + replace_string + '.pkl'
    print(save_string)
    pickle.dump(xml, open(save_string, "wb" ))
    return True



def rotate_pdf(full_path):
    out_path = 'F:/Environmental Baseline Data/Version 4 - Final/PDF_rotated/'
    pdf_in = open(full_path, 'rb')
    pdf_reader = PyPDF2.PdfFileReader(pdf_in, strict=False)
    pdf_writer = PyPDF2.PdfFileWriter()
    for pagenum in range(pdf_reader.numPages):
        page = pdf_reader.getPage(pagenum)
        page.rotateClockwise(90)
        pdf_writer.addPage(page)
    pdf_out = open(out_path + full_path.split('/')[-1], 'wb')
    pdf_writer.write(pdf_out)
    pdf_out.close()
    pdf_in.close()
    return True

