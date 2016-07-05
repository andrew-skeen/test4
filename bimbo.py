# -*- coding: utf-8 -*-
"""
Created on Tue Jun 14 11:39:24 2016

@author: andrew
"""

import re
import gzip as gz
import numpy as np
import pdb
import datetime
import pandas as pd

def read_file(f_obj):
    for line in f_obj:
        yield line
   
"""     
def read_file(file_object):
    while True:
        data = file_object.readline()
        if not data:
            break
        yield data
"""
path='/home/andrew/bimbo/'

outfile='sample.txt'

#******************************************************************************
# header translation
#******************************************************************************

uni2ascii = {
            ord('\xe2\x80\x99'.decode('utf-8')): ord("'"),
            ord('\xe2\x80\x9c'.decode('utf-8')): ord('"'),
            ord('\xe2\x80\x9d'.decode('utf-8')): ord('"'),
            ord('\xe2\x80\x9e'.decode('utf-8')): ord('"'),
            ord('\xe2\x80\x9f'.decode('utf-8')): ord('"'),
            ord('\xc3\xa9'.decode('utf-8')): ord('e'),
            ord('\xe2\x80\x9c'.decode('utf-8')): ord('"'),
            ord('\xe2\x80\x93'.decode('utf-8')): ord('-'),
            ord('\xe2\x80\x92'.decode('utf-8')): ord('-'),
            ord('\xe2\x80\x94'.decode('utf-8')): ord('-'),
            ord('\xe2\x80\x94'.decode('utf-8')): ord('-'),
            ord('\xe2\x80\x98'.decode('utf-8')): ord("'"),
            ord('\xe2\x80\x9b'.decode('utf-8')): ord("'"),

            ord('\xe2\x80\x90'.decode('utf-8')): ord('-'),
            ord('\xe2\x80\x91'.decode('utf-8')): ord('-'),

            ord('\xe2\x80\xb2'.decode('utf-8')): ord("'"),
            ord('\xe2\x80\xb3'.decode('utf-8')): ord("'"),
            ord('\xe2\x80\xb4'.decode('utf-8')): ord("'"),
            ord('\xe2\x80\xb5'.decode('utf-8')): ord("'"),
            ord('\xe2\x80\xb6'.decode('utf-8')): ord("'"),
            ord('\xe2\x80\xb7'.decode('utf-8')): ord("'"),

            ord('\xe2\x81\xba'.decode('utf-8')): ord("+"),
            ord('\xe2\x81\xbb'.decode('utf-8')): ord("-"),
            ord('\xe2\x81\xbc'.decode('utf-8')): ord("="),
            ord('\xe2\x81\xbd'.decode('utf-8')): ord("("),
            ord('\xe2\x81\xbe'.decode('utf-8')): ord(")")}

def unicodetoascii(text):
    return text.decode('utf-8').translate(uni2ascii).encode('ascii')

txt='''Semana — Week number (From Thursday to Wednesday)
    Agencia_ID — Sales Depot ID
    Canal_ID — Sales Channel ID
    Ruta_SAK — Route ID (Several routes = Sales Depot)
    Cliente_ID — Client ID
    NombreCliente — Client name
    Producto_ID — Product ID
    NombreProducto — Product Name
    Venta_uni_hoy — Sales unit this week (integer)
    Venta_hoy — Sales this week (unit: pesos)
    Dev_uni_proxima — Returns unit next week (integer)
    Dev_proxima — Returns next week (unit: pesos)
    Demanda_uni_equil — Adjusted Demand (integer) (This is the target you will predict)'''

txt=unicodetoascii(txt)

txt=txt.split('\n')
    
capture=[re.findall(r'([a-zA-Z_]+)\s*\-\s*([a-zA-Z\s]+)\s*[^a-zA-Z\s]*.*$', t)[0] for t in txt]
capture={re.sub('\s','_', elt[1]): elt[0] for elt in capture}

#******************************************************************************
# read file seuqntially
#******************************************************************************

keys=['Client_ID', 'Product_ID', 'Adjusted_Demand_']

summary={}
with open(path+'train.csv') as f:
    rows=0
    for line in read_file(f):
        line=line.strip().split(',')
        if rows==0:
            ids=[ [index for index, val in enumerate(line) if capture[k]==val][0] for k in keys]
        else:
            client, product, demand=[line[i] for i in ids]
            summary[('a',product)]=summary.get(('a',product), 0)+float(demand)
        #pdb.set_trace()
        rows+=1
        if rows%500000==0:
            print('Done %s' % (str(rows)))
            #print(len(summary.keys()))

import pandas as pd
index, values=zip(*summary.items())

summary_s=pd.Series(values, index=index)

summary_s.sort_values(ascending=False, inplace=True)

_,products=zip(*summary_s.index)

summary_s=pd.DataFrame({'prod': products, 'cnt': summary_s.values})


summary_s['cumsum']=summary_s.cnt.cumsum()

total=summary_s.cumsum.max()

num_files=10

from math import floor, ceil
overall={}
prod_list=[]
file_num=0
for row in xrange(summary_s.shape[0]):
    if (file_num==9):
        #pdb.set_trace()
        pass
    if (summary_s['cumsum'].ix[row]<= (file_num+1)*ceil(total/num_files)) :
        prod_list.append(summary_s['prod'].ix[row])
    else:
        overall[file_num]=prod_list
        file_num+=1
        prod_list=[summary_s['prod'].ix[row]]
overall[file_num]=prod_list 

overall=[([key]*len(val), val) for key, val in overall.items()]

overall=[pd.DataFrame({'grp':key, 'prod': val}) for key, val in overall]

overall_d=reduce(lambda x,y: pd.concat((x,y), axis=0), overall)
overall_d['prod']=overall_d['prod'].astype('int')

overall_d={int(x[1]): x[0] for x in overall_d.values }


file_obj=[open(path+'train_%s.csv'%(str(k)), 'w') for k in xrange(10)]


with open(path+'train.csv') as f:
    rows=0
    for line in read_file(f):
        if rows==0:
            header=line
            h_split=header.strip().split(',')
            #pdb.set_trace()
            index=[i for i,h in enumerate(h_split) if h=='Producto_ID'][0]
            [f.write(header) for f in file_obj]
        else:
            #i=overall_d.grp[overall_d['prod']==int(line.strip().split(',')[index])].values
            i=overall_d[int(line.strip().split(',')[index])]
            #pdb.set_trace()
            file_obj[i].write(line)
        rows+=1
        if rows%500000==0:
            print('Done %s' % (str(rows)))

for f in file_obj:
    f.close()

    
    
    


summary=summary