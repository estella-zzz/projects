# -*- coding: utf-8 -*-
# <nbformat>4</nbformat>

# <markdowncell> {}

# # Tableay Workbook Review
# ### Review your Workbook info, datasources, dashboards and field usage

# <codecell> {}

!pip install tableauserverclient
!pip install tableaudocumentapi

# <codecell> {}

from lxml import etree
import pandas as pd
import numpy as np
import re
# from dataviz.table import DataTable
import tableauserverclient as TSC
import os, traceback
from tableaudocumentapi import Workbook,Datasource,Field
import shutil
import zipfile
import glob
import getpass

# <markdowncell> {}

# ### Get the workbook from Tableau Server / Upload from your computer
# Server:
# <br>url:

# 
# Local:
# <br>file_path = "/home/Downloads/workbook.twbx"

# <codecell> {}

local = False

# <codecell> {}

###Input

if local == False:
    ldap = input("ldap: ")
    pwd = getpass.getpass("pwd: ")
    url = input("url:")
    
    ##Connection
    tableau_auth = TSC.TableauAuth(ldap,pwd)
    server = TSC.Server('https://public.tableau.com')
    server.auth.sign_in(tableau_auth)
else:
    file_path = input("file path:")

# <codecell> {}

###Variables
dash=[]
workbooks = []
worksheetdatasource=[]

# <codecell> {}

# os.getcwd()

# <codecell> {}

# server.auth.sign_in(tableau_auth)

# <codecell> {}

###get tree server
try:
    ##Download Workbook
    if local == False:
        for wb in TSC.Pager(server.workbooks):
            if wb.content_url == url:
                workbook = wb
        workbooks.append([workbook.id,workbook.name,workbook.owner_id,workbook.project_id,workbook.project_name])
        file_path = server.workbooks.download(workbook.id,no_extract=True)
        n = workbook.name
        print("Downloaded the file to {}".format(file_path))
    else:
        ##get tree local
        n = file_path.split("/")[len(file_path.split("/"))-1].split(".")[-2]
#         root = tree.getroot()
    extension=file_path.split(".")[len(file_path.split("."))-1]
    wb=""
    tree=""
    ##Manage extension
    if extension == "twb":
        wb = Workbook(file_path)
        tree = etree.parse(file_path)
        os.remove(file_path)
    else:
        if local == False:
            mzip = os.rename(file_path,"temp.zip" )            
        else:
            mzip = shutil.copyfile(file_path,'temp.zip')

        zip = zipfile.ZipFile("temp.zip")
        zip=zip.extractall('temp')
        name=""
        for f in glob.glob('temp/*.twb'):
            name=f
        wb = Workbook(name)
        tree = etree.parse(name)
        os.remove("temp.zip")
        shutil.rmtree("temp")
    
    root = tree.getroot()
    
    for dashboard in root.findall("./windows/window/[@class='dashboard']"):
        #    print(dashboard.attrib["name"])
        for viewpoints in dashboard:
            for viewpoint in viewpoints:
                #            print viewpoint.attrib["name"]
                dash.append([n,dashboard.attrib["name"],viewpoint.attrib["name"]])    
    
    for datasource in wb.datasources:
        if datasource.name !="Parameters":
            for count, field in enumerate(datasource.fields.values()):
                worksheetdatasource.append([datasource.caption,field.name,field.caption,field.datatype,field.role,field.alias,field.calculation,len(field.worksheets)])
    # for c in root.findall("./datasources/datasource/connection/metadata-records/metadata-record/[@class='column']"):
    #     for d in c:
    #         print(d.text)
except Exception:
    print("Issue with {0}",workbook.name)
    traceback.print_exc()
    pass
try:
    os.remove("temp.zip")
    shutil.rmtree("temp")
except Exception:
    pass

if local == False:
    server.auth.sign_out()

# <codecell> {}

# worksheetdatasource.loc[worksheetdatasource['Field Calculation'] == '[Calculation_1002262061149728768]']

# <codecell> {}

##Build Dataframe
cols=['Datasource Name','Field Name','Field Caption','Field Datatype',"Field Role","Field Alias","Field Calculation","# Usage in Worksheet"]
worksheetdatasource = pd.DataFrame(worksheetdatasource, columns=cols) 
cols=['Workbook Name','Dashboard','Worksheet']
dashboardf = pd.DataFrame(dash, columns=cols)
cols=['Workbook LUID','workbook Name','Owner ID','Project ID','Project Name']
workbooks = pd.DataFrame(workbooks, columns=cols) 
try:
    os.remove("metadata.csv")
    os.remove("dashboard.csv")
    os.remove("storyboard.csv")
    os.remove("workbooks.csv")
except OSError:
    pass

# worksheetdatasource=worksheetdatasource.drop_duplicates(subset=['Workbook LUID','Worksheet','Datasource Name','Field Name'], keep=False)

# <codecell> {}

# result = etree.tostring(tree.getroot(), pretty_print=True, method="html")
# print(result)

# <codecell> {}

### get the elements for all worksheets
worksheetelem = root.findall(".//worksheet")

### parse!
captions = {}
calculations = {}
params = []
# for sheet in worksheetelem:
# #     print(sheet.tag, sheet.attrib['name'])
#     for x in sheet.findall('.//column'):
#         name = x.get('name')
#         caption = x.get('caption')
#         if x.get('param-domain-type') != None:
#             params.append(caption)
# ### get all column captions
#         if name is not None: 
#             if name not in captions:
#                 captions[name] = caption
#             elif captions[name] != caption:
#                 captions[name] = 'dupe'
# ### get all calculation fields
#             if (x.findall('./calculation') != []) & (name != '[Number of Records]'):
#                 calc = x.findall('./calculation')[0]
#                 formula = calc.get('formula')
#                 if (formula != None):
#                     if (name not in calculations):
#                         calculations[name] = formula
#                     elif (calculations[name] != formula):
#                         calculations[name] = 'dupe'

# <codecell> {}

ds_all = {}
for sheet in worksheetelem:
    for ds in sheet.findall('.//datasource'):
        ds_all[ds.get('caption')] = {}
        calculations = {}
        captions = {}
        for x in sheet.findall('.//column'):
            name = x.get('name')
            caption = x.get('caption')
            if x.get('param-domain-type') != None:
                params.append(caption)
    ### get all column captions
            if name is not None: 
                if name not in captions:
                    captions[name] = caption
                elif captions[name] != caption:
                    print(name, caption, captions[name])
#                     captions[name] = 'dupe'
    ### get all calculation fields
                if (x.findall('./calculation') != []) & (name != '[Number of Records]'):
                    calc = x.findall('./calculation')[0]
                    formula = calc.get('formula')
                    if (formula != None):
                        if (name not in calculations):
                            calculations[name] = formula
                        elif (calculations[name] != formula):
                            print(name, formula, calculations[name])
#                             calculations[name] = 'dupe'
        ds_all[ds.get('caption')]['calculations'] = calculations
        ds_all[ds.get('caption')]['captions'] = captions

# <codecell> {}

params = set(params)

# <codecell> {}

# len(captions)

# <codecell> {}

ds_clean = {}
for key in ds_all:
    ds_clean[key] = {}
    captions = ds_all[key]['captions']
    calculations = ds_all[key]['calculations']
    ### get all fields that have a caption
    
    match_dict = {}
    for (keys, values) in captions.items():
        if values is not None:
    #         values = '['+values+']'
            match_dict[keys] = values

    ### regex list
    re_list = []
    for x in match_dict.keys():
        y = x.replace('[','\[')
        y = y.replace(']','\]')
        y = y.replace('(','\(')
        y = y.replace(')','\)')
        re_list.append(y)

    # re_list[0:10]
    # calculations

    ### get clean calculation field list and formula
    calc_clean = {}
    for x in calculations:
        formula = calculations[x]    
        for i in re_list:
            sub = (re.findall(i,calculations[x]))
            if sub != []:
                for field in sub:
                    formula = formula.replace(field, match_dict[field])
        calc_clean[match_dict[x]] = formula
    ds_clean[key] = calc_clean

# <codecell> {}

data  = pd.DataFrame(columns = ['field_name','formula','Datasource Name'])
for key in ds_clean:
    tmp = pd.DataFrame.from_dict(ds_clean[key], orient = 'index', columns = ['formula']).reset_index()
    tmp.rename(columns = {'index':'field_name'}, inplace = True)
    tmp['Datasource Name'] = key
    data = data.append(tmp, ignore_index = True)

# <codecell> {}

# data = pd.DataFrame.from_dict(calc_clean, orient = 'index', columns = ['formula']).reset_index()
# data.rename(columns = {'index':'field_name'}, inplace = True)
# data.head()

# <codecell> {}

# worksheetdatasource.head()

# <codecell> {}

datasourceusage = worksheetdatasource.merge(data, how = 'left', left_on = ['Field Name','Datasource Name'],\
                                            right_on = ['field_name','Datasource Name'])

datasourceusage['Field Calculation'] = datasourceusage['formula'].combine_first(datasourceusage['Field Calculation'])

datasourceusage.drop(columns = ['formula','Field Caption','field_name'], inplace = True)

datasourceusage['Field Role'] = datasourceusage.apply(lambda x: 'parameter' if x['Field Name'] in params\
                                                      else x['Field Role'], axis = 1)

# <markdowncell> {}

# # Workbook Information

# <codecell> {}

output_wb = JoogleChart(workbooks)
output_wb.show('Table')

# <markdowncell> {}

# sample:
# <img src="files/Screen Shot 2020-03-16 at 11.48.31 PM.PNG">

# <codecell> {}



# <markdowncell> {}

# # Dashboards & Worksheets

# <codecell> {}

output_dash = JoogleChart(dashboardf)
output_dash.show('Table')

# <markdowncell> {}

# sample:
# <img src="files/Screen Shot 2020-03-16 at 11.53.35 PM.PNG">

# <markdowncell> {}

# # Fields & Calculations

# <codecell> {}

output_fields = JoogleChart(datasourceusage.fillna('-'))
output_fields.show('Table')
# datasourceusage.loc[(datasourceusage['# Usage in Worksheet'] == 0) & (~datasourceusage['Field Calculation'].isnull())]

# <markdowncell> {}

# sample:
# <img src="files/Screen Shot 2020-03-16 at 11.54.16 PM.PNG">

# <codecell> {}

##Export
datasourceusage.to_csv("metadata.csv", sep='\t', encoding='utf-8')
dashboardf.to_csv("dashboard.csv", sep='\t', encoding='utf-8')
workbooks.to_csv("workbooks.csv", sep='\t', encoding='utf-8')

# <codecell> {}

# ##dashboard and sheets###
# dashlist = root.findall(".//dashboard")
# for dash in dashlist:
#     print('\n', dash.get('name'))
#     for elem in dash.findall('.//zone'):
#         sheet = elem.get('name')
#         type = elem.get('type')
#         if (sheet is not None) & (type != 'filter'):
#             print(sheet)
            
# for dashboard in root.findall("./windows/window/[@class='dashboard']"):
#     #    print(dashboard.attrib["name"])
#     for viewpoints in dashboard:
#         for viewpoint in viewpoints:
#             #            print viewpoint.attrib["name"]

# <codecell> {}

# test = root.findall(".//datasource")
# for x in test:
#     if x.get('inline') == 'true':
#         print (x.get('caption'), x.attrib)
#         columns = x.findall('.//metadata-record')
#         if columns != []:
#             for x in columns:
#                 if x.get('class') == 'column':
#                     if x.find('.//caption') != None:
#                         print(x.find('.//caption').text)
#                     else:
#                         print(x.find('.//local-name').text, ' no-caption')
#                     print(x.find('.//local-type').text)
#         query
#         if (x.findall('.//relation')) != []:
#             print(x.findall('.//relation')[0].text)

# <metadatacell>

{"kernelspec": {"display_name": "Python 3", "name": "python3", "language": "python"}}