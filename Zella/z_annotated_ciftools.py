# -*- coding: utf-8 -*-
"""
Definitions of classes, functions and methods for pulling cancer data

To be imported as a module in CIF_pull_data.py or CIF_pull_data.ipynb
"""

__authors__ = ["Todd Burus, MAS", "Lee Park, MS"]
__copyright__ = "Copyright 2022, University of Kentucky"

import pandas as pd
import requests
import re
import numpy as np
from bs4 import BeautifulSoup
import time
import os
from io import BytesIO
from zipfile import ZipFile
import urllib.request
import glob

pd.set_option('display.max_colwidth', 0)

#################################################
### customize paths and files before running  ###
#################################################

### insert ACS API key
acs_key = '6888c47d7cd57defbcd47c6e6b71df8c70f6cccc'

#################################################

### define CIFTools class
class CIFTools:
    # store key
    def __init__(self, key = None):
        if key == None:
            self.insert_key()
        else:
            self.key = key
        self.census_available_region = np.array(['state','county',
                                                 'county subdivision','tract',
                                                 'block', 'zip'])
        self.tables_basket = None

    def insert_key(self):
        try:
            print(self.key)
        except:
            self.key = ""


    # check that chosen region is available
    def find_census_region(self, search_value):
        pattern = re.compile(f'{search_value}$', flags = re.I)
        index = list(map(lambda x: bool(re.search(pattern, x)), self.census_available_region))
        return(self.census_available_region[index][0])


    # display variables in ACS5
    def get_acs_groups(self, year= None):
        if year == None:
            year = self.year
        response_acs5 = requests.get(f'https://api.census.gov/data/{year}/acs/acs5/groups')
        response_profile = requests.get(f'https://api.census.gov/data/{year}/acs/acs5/profile/groups')
        try:
            response_acs5.raise_for_status()
            response_profile.raise_for_status()
        except:
            print('check the group json website')
        acs5 = pd.DataFrame(response_acs5.json()['groups'])
        profile = pd.DataFrame(response_profile.json()['groups'])
        acs5['source'] = 'acs5'
        profile['source'] = 'profile'
        groups = pd.concat([acs5, profile], ignore_index = True)
        groups = groups.iloc[:, [0,1,3]]
        self.groups = groups
        return(self.groups)



    def gen_acs_groups(self, year):
        response_acs5 = requests.get(f'https://api.census.gov/data/{year}/acs/acs5/groups')
        response_profile = requests.get(f'https://api.census.gov/data/{year}/acs/acs5/profile/groups')
        try:
            response_acs5.raise_for_status()
            response_profile.raise_for_status()
        except:
            print('check the group json website')
        acs5 = pd.DataFrame(response_acs5.json()['groups'])
        profile = pd.DataFrame(response_profile.json()['groups'])
        acs5['source'] = 'acs5'
        profile['source'] = 'profile'
        groups = pd.concat([acs5, profile], ignore_index = True)
        groups = groups.iloc[:, [0,1,3]]
        self.groups = groups



    def drop(self, dataframe, colname, group = False):
        if group:
            pattern = re.compile(colname, flags = re.I)
            colname = dataframe.columns.to_series()
            index = colname.str.match(pattern)
            colname_to_drop = colname[index]
            return(dataframe.drop(colname_to_drop))
        else:
            return(dataframe.drop(colname))




    def gen_pattern(self, subgroup_type):
        if subgroup_type.lower() == 'sex':
            pattern = re.compile('.*sex.*', flags = re.I)
        elif subgroup_type.lower() == 'age':
            pattern = re.compile('.*age.*', flags = re.I)
        elif subgroup_type.lower() == 'both':
            pattern = re.compile('(?=.*sex.*)(?=.*age.*)', flags = re.I)
        else:
            pattern = None
        return(pattern)

class acs(CIFTools):
    def __init__(self, Key = acs_key):
    # Run the init function of the parent class first
        super().__init__(key = Key)
        self.year = 2019
        self.source = 'acs/acs5'
        self.region = 'tract'
        self.state = '21'

        state = '''Name   FIPS   State
Alabama	01	AL
Alaska	02	AK
Arizona	04	AZ
Arkansas	05	AR
California	06	CA
Colorado	08	CO
Connecticut	09	CT
Delaware	10	DE
District of Columbia	11	DC
Florida	12	FL
Georgia	13	GA
Hawaii	15	HI
Idaho	16	ID
Illinois	17	IL
Indiana	18	IN
Iowa	19	IA
Kansas	20	KS
Kentucky	21	KY
Louisiana	22	LA
Maine	23	ME
Maryland	24	MD
Massachusetts	25	MA
Michigan	26	MI
Minnesota	27	MN
Mississippi	28	MS
Missouri	29	MO
Montana	30	MT
Nebraska	31	NE
Nevada	32	NV
New Hampshire	33	NH
New Jersey	34	NJ
New Mexico	35	NM
New York	36	NY
North Carolina	37	NC
North Dakota	38	ND
Ohio	39	OH
Oklahoma	40	OK
Oregon	41	OR
Pennsylvania	42	PA
Rhode Island	44	RI
South Carolina	45	SC
South Dakota	46	SD
Tennessee	47	TN
Texas	48	TX
Utah	49	UT
Vermont	50	VT
Virginia	51	VA
Washington	53	WA
West Virginia	54	WV
Wisconsin	55	WI
Wyoming	56	WY'''
        states = pd.DataFrame([x.split(',') for x in state.replace('\t', "  ").replace('  ',',').split('\n')])
        states.columns = states.iloc[0,:]
        states = states.iloc[1:,:]
        states.columns = states.columns.str.strip()
        self.state_fips = states
        del states

    def __repr__(self):
        message = 'This module is to pull and search the American Community Survey Data; for more information, please visit https://github.com/leeparkuky/MarkeyDataTools'
        return(message)

    def guide(self):
        year = int(input('what year was the dataset created:'))
        group = ""
        groups = []
        while group.lower() != "quit":
            group = str(input('provide a group name to include in your dataset [when you finished, please submit "quit"'))
            groups.append(group)
        acs = 'acs5'
        response = str(input('Is the dataset "acs5"? [Yes/No]'))
        if response.lower() == 'no':
            acs = str(input('What is the dataset of interest:'))
        geo_response = str(input('What is the geological level? [State, County, County Subdivision, Tract] :'))
        self.insert_inputs(year, table = groups, source = acs, region = geo_response)


    def validate_attributes(self):
        # validating the source attribute
        source = self.source
        if bool(re.match('acs/acs\d', source)):
            pass
        elif source.lower() in ['acs5','subject','profile']:
            if source.lower() == 'acs5':
                source = '/'.join(['acs', source])
            else:
                source = '/'.join(['acs/acs5', source])
        self.source = source

        table = self.table


        try:
            self.variable_table
        except:
            self.gen_variable_table()


        if type(table) == str:
            table = self.find_variable_list(table)
        elif type(table) == list:
            table = pd.concat((list(map(self.find_variable_list, table))))
        self.table_list = table
        self.source = self.variable_table.loc[self.variable_table.name.str.match(self.table),'source'].values[0]

        # validating the state attribute
        self.check_self_state()

        # cleaning region
        region = self.region
        self.region = self.find_census_region(region)


    def check_self_state(self):
        state = self.state
        try:
            int(state) + 1
        except:
            if (len(state) > 2) & (type(state) == str):
                self.state = self.state_fips.loc[self.state_fips.Name.eq(state),'FIPS'].astype(int).values[0]
            elif (type(state) == str) & (len(state) == 2):
                self.state = self.state_fips.loc[self.state_fips.State.eq(state), 'FIPS'].astype(int).values[0]



    def insert_inputs(self, year, table, source = 'acs5', state = 'KY', region = 'County'):
        # assign values to the following attributes
        self.year = year


        # complying with the API syntax for the sources.
        if bool(re.match('acs/acs\d', source)):
            pass
        elif source.lower() in ['acs5','subject','profile']:
            if source.lower() == 'acs5':
                source = '/'.join(['acs', source])
            else:
                source = '/'.join(['acs/acs5', source])
        self.source = source

        # generating the variable table first
        self.gen_variable_table()

        # table names has to follow the syntax for the API call
        if type(table) == str:
            table = self.find_variable_list(table)
        elif type(table) == list:
            table = pd.concat((list(map(self.find_variable_list, table))))


    def insert_table(self, table):
        # generating the variable table first
        self.gen_variable_table()

        # table names has to follow the syntax for the API call
        if type(table) == str:
            table = self.find_variable_list(table)
        elif type(table) == list:
            table = pd.concat((list(map(self.find_variable_list, table))))

        self.table_list = table



    def acs5_variables(self):
        try:
            (self.year)
        except:
            self.year = 2019
        response = requests.get(f'https://api.census.gov/data/{self.year}/acs/acs5/variables')
        lists = response.json()
        header , values = lists[0], lists[1:]
        return(pd.DataFrame(values, columns = header))




    def sub_group_search(self, keyword_regex = None):
        try:
            self.year
        except:
            self.year = 2019
        response = requests.get(f'https://api.census.gov/data/{self.year}/{self.source}/subject/groups')
        json = response.json()['groups']
        table = pd.DataFrame(json).sort_values('name').reset_index(drop = True)
        if keyword_regex == None:
            return(table)
        else:
            pattern = re.compile(f'.*({keyword_regex}).*', flags = re.I)
            finding = table.loc[table.description.str.match(pattern),:]
            return(finding)

    def group_search(self, keyword_regex = None, B = True, subgroups = None):
        '''possible choices of subgroups are sex, age, and both'''#################################################################
        try:
            self.year
        except:
            self.year = 2019
        pattern = re.compile(f'(\s+({keyword_regex}).*)|(^({keyword_regex}).*)', flags = re.I)
        if subgroups == None:
            subgroups_pattern = re.compile('.*')
        else:
            subgroups_pattern = self.gen_pattern(subgroups)
        try:
            group_number = self.sub_group_search(keyword_regex).name.str.slice(1, 3).iloc[0,]
        except:
            if subgroups == None:
                name_pattern = re.compile('B.*00[123]$')
            else:
                name_pattern = re.compile('B\d+$')
            response = requests.get(f'https://api.census.gov/data/{self.year}/{self.source}/groups')
            json = response.json()['groups']
            table = pd.DataFrame(json).sort_values('name').reset_index(drop = True)
            if keyword_regex == None:
                if B:
                    table = table.loc[table.name.str.match(name_pattern),:]
                return(table)
            else:
                pattern = re.compile(f'.*({keyword_regex}).*', flags = re.I)
                if B:
                    finding = table.loc[table.name.str.match(name_pattern)&table.description.str.match(pattern)&table.description.str.match(subgroups_pattern),:]
                else:
                    finding = table.loc[table.description.str.match(pattern)&table.description.str.match(subgroups_pattern),:]
                return(finding)
        else:
            group_number = self.sub_group_search(keyword_regex).name.str.slice(1, 3).iloc[0,]
            name_pattern = re.compile(f'B{group_number}\d+$')
            response = requests.get(f'https://api.census.gov/data/{self.year}/{self.source}/groups')
            json = response.json()['groups']
            table = pd.DataFrame(json).sort_values('name').reset_index(drop = True)
            if keyword_regex == None:
                if B:
                    table = table.loc[table.name.str.match(name_pattern)&table.description.str.match(subgroups_pattern),:]
                return(table)
            else:
                pattern = re.compile(f'.*({keyword_regex}).*', flags = re.I)
                if B:
                    finding = table.loc[table.name.str.match(name_pattern)&table.description.str.match(pattern)&table.description.str.match(subgroups_pattern),:]
                else:
                    finding = table.loc[table.description.str.match(pattern)&table.description.str.match(subgroups_pattern),:]
                return(finding)



    # This generates the variable table for the source of the data
    def gen_variable_table(self):
        try:
            a = self.source
        except:
            self.source = 'acs/acs5'

        response = requests.get(f'https://api.census.gov/data/{self.year}/acs/acs5/variables')
        lists = response.json()
        header, values = lists[0], lists[1:]
        acs5 = pd.DataFrame(values, columns = header)
        acs5['source'] = 'acs/acs5'

        response = requests.get(f'https://api.census.gov/data/{self.year}/acs/acs5/profile/variables')
        lists = response.json()
        header, values = lists[0], lists[1:]
        subject = pd.DataFrame(values, columns = header)
        subject['source'] = 'acs/acs5/profile'

        response = requests.get(f'https://api.census.gov/data/{self.year}/acs/acs5/subject/variables')
        lists = response.json()
        header, values = lists[0], lists[1:]
        profile = pd.DataFrame(values, columns = header)
        profile['source'] = 'acs/acs5/subject'


        self.variable_table =  pd.concat([acs5, subject,profile])




    # from the variable table, this function allows you to find relevant variables from the 'table' argument you set up.
    def find_variable_list(self, var_name):
        pattern = re.compile(f'{var_name}_\d+[0-9]E')
        sub_variables = self.variable_table.loc[self.variable_table.name.str.match(pattern),:]
        #size = sub_variables.shape[0]
        names = sub_variables.name
        return(names)



    def refresh(self):
        self.acs_data = self.acs_data.sort_values('FIPS').reset_index(drop = True)



    # This generates the data frame using the arguments
    def gen_dataframe(self, return_table = False):
        n = 49
        result = None
        self.table_list = self.table_list.sort_values().reset_index(drop = True)
        if self.table_list.shape[0] > n:
            i = int(np.floor(self.table_list.shape[0]/n))
            for j in range(i+1):
                if j < i:
                    table = self.table_list[n*j:n*(j+1)]
                    table = ','.join(table)
                    pattern = re.compile(',,+')
                    table = re.sub(pattern, '', table)
                    if j == 0:
                        result = self.gen_single_frame(table)
                    else:
                        result = result.merge(self.gen_single_frame(table), on = 'FIPS', suffixes=('', '_todrop') )
                        result = result.drop(result.columns[result.columns.str.match(re.compile('.*_todrop'))].to_list(), axis = 1)
                else:
                    table = self.table_list[n*j:]
                    table = ','.join(table)
                    pattern = re.compile(',,+')
                    table = re.sub(pattern, '', table)
                    result = result.merge(self.gen_single_frame(table), on = 'FIPS', suffixes=('', '_todrop') )
                    result = result.drop(result.columns[result.columns.str.match(re.compile('.*_todrop'))].to_list(), axis = 1)
            result = result.loc[:,result.columns.to_series()[~result.columns.str.match(re.compile('.*_todrop'))]]
            self.acs_data = result
            self.colname = self.acs_data.columns

        else:
            table = self.table_list
            table = ','.join(table)
            pattern = re.compile(',,+')
            table = re.sub(pattern, '', table)
            result = self.gen_single_frame(table)
            self.acs_data = result
            self.colname = self.acs_data.columns

        k=4
        if self.region == 'block':
            k  = 5
        for i in range(k, result.shape[1]):
            try:
                self.acs_data.iloc[:,i] = self.acs_data.iloc[:,i].astype(float)
            except:
                try:
                    self.acs_data.iloc[:,i] = self.acs_data.iloc[:,i].astype(float)
                except:
                    pass
            else:
                self.acs_data.iloc[:,i] = self.acs_data.iloc[:,i].astype(float)

        if return_table:
            return(self.acs_data)





    def gen_single_frame(self, table):
        if self.region == 'state':
            self.acs_url = f'https://api.census.gov/data/{self.year}/{self.source}?get={table}&for=state:{self.state}&key={self.key}'
            response = requests.get(f'https://api.census.gov/data/{self.year}/{self.source}?get={table}&for=state:{self.state}&key={self.key}')
        elif self.region == 'county':
            self.acs_url = f'https://api.census.gov/data/{self.year}/{self.source}?get=NAME,{table}&for=county:*&in=state:{self.state}&key={self.key}'
            response = requests.get(f'https://api.census.gov/data/{self.year}/{self.source}?get=NAME,{table}&for=county:*&in=state:{self.state}&key={self.key}')
        elif self.region == 'county subdivision':
            self.acs_url = f'https://api.census.gov/data/{self.year}/{self.source}?get=NAME,{table}&for=county%20subdivision:*&in=state:{self.state}&in=county:*&key={self.key}'
            response = requests.get(f'https://api.census.gov/data/{self.year}/{self.source}?get=NAME,{table}&for=county%20subdivision:*&in=state:{self.state}&in=county:*&key={self.key}')
        elif self.region == 'tract':
            self.acs_url = f'https://api.census.gov/data/{self.year}/{self.source}?get=NAME,{table}&for=tract:*&in=state:{self.state}&in=county:*&key={self.key}'
            response = requests.get(f'https://api.census.gov/data/{self.year}/{self.source}?get=NAME,{table}&for=tract:*&in=state:{self.state}&in=county:*&key={self.key}')
        elif self.region == 'block':
            self.acs_url = f'https://api.census.gov/data/{self.year}/{self.source}?get=NAME,{table}&for=block%20group:*&in=state:{self.state}&in=county:*&in=tract:*&key={self.key}'
            response= requests.get(f'https://api.census.gov/data/{self.year}/{self.source}?get=NAME,{table}&for=block%20group:*&in=state:{self.state}&in=county:*&in=tract:*&key={self.key}')
        elif self.region == 'zip':
            self.acs_url = f'https://api.census.gov/data/{self.year}/{self.source}?get=NAME,{table}&for=zip%20code%20tabulation%20area:*&in=state:{self.state}&key={self.key}'
            response = requests.get(f'https://api.census.gov/data/{self.year}/{self.source}?get=NAME,{table}&for=zip%20code%20tabulation%20area:*&in=state:{self.state}&key={self.key}')
        else:
            print('The region level is not found in the system')

        # separate the header and the values in the list of lists
        self.something_to_check = table
        self.response = response
        header = response.json()[0]
        values = response.json()[1:]
        df = pd.DataFrame(values, columns = header)

        if self.region == 'state':
            df.drop(['state'], axis = 1, inplace = True)

        elif self.region == 'county':
            df['FIPS'] = df.state + df.county
            df.drop(['state','county'], axis = 1, inplace = True)
            df[['County', 'State']] = df.NAME.str.split(pat = ', ', expand = True)
            df.drop('NAME', axis = 1, inplace = True)
            colnames = pd.concat([pd.Series(df.columns[-3:]), pd.Series(df.columns[:-3])])
            df = df[colnames]

        elif self.region == 'county subdivision':
            df['FIPS'] = df['state'] + df['county'] + df['county subdivision']
            df.drop(['state','county', 'county subdivision'], axis = 1, inplace = True)
            df[['County Subdivision', 'County', 'State']] = df.NAME.str.split(pat = ', ', expand = True)
            df.drop('NAME', axis = 1, inplace = True)
            colnames = pd.concat([pd.Series(df.columns[-4:]), pd.Series(df.columns[:-4])])
            df = df[colnames]

        elif self.region == 'tract':
            df['FIPS'] = df['state'] + df['county'] + df['tract']
            df.drop(['state','county', 'tract'], axis = 1, inplace = True)
            df[['Tract', 'County', 'State']] = df.NAME.str.split(pat = ', ', expand = True)
            df.drop('NAME', axis = 1, inplace = True)
            colnames = pd.concat([pd.Series(df.columns[-4:]), pd.Series(df.columns[:-4])])
            df = df[colnames]

        elif self.region == 'block':
            df['FIPS'] = df['state'] + df['county'] + df['tract'] + df['block group']
            df.drop(['state','county', 'tract','block group'], axis = 1, inplace = True)
            df[['Block', 'Tract', 'County', 'State']] = df.NAME.str.split(pat = ', ', expand = True)
            df.drop('NAME', axis = 1, inplace = True)
            colnames = pd.concat([pd.Series(df.columns[-5:]), pd.Series(df.columns[:-5])])
            df = df[colnames]
        elif self.region == 'zip':
            df.drop(['NAME'], axis = 1, inplace = True)
            df.rename(columns = {"zip code tabulation area":'zip_code'}, inplace = True)
            colnames = df.columns.to_list()[-2:] + df.columns.to_list()[:-2]
            df = df.loc[:,colnames]
        return(df)


    # You can search variable groups that might contain information of interest defined by the keyword
    # For now, when the user gives a simple string, comma indicates "OR"
    # It also accepts the regex pattern. However, users must make sure they are in the re.Pattern type
    def search(self, keyword, savefile = False, filename = ''):
        try:
            self.groups
        except:
            self.get_acs_groups(self.year)
        if type(keyword) == re.Pattern:
            self.search_result = self.groups.loc[self.groups.description.str.match(keyword), :]
        else:
            keyword = keyword.split(',')
            keyword = list(map(lambda x: '('+x+')', keyword))
            keyword = '|'.join(keyword)
            pattern = re.compile(f'.*({keyword}).*', flags = re.I)
            self.search_result = self.groups.loc[self.groups.description.str.match(pattern), :]
        self.search_result.reset_index(inplace = True)
        if savefile == True:
            if filename == '':
                filename = str(input("Please enter the filename: "))
                self.search_result.to_csv(filename, index = False)
            else:
                self.search_result.to_csv(filename, index = False)
        else:
            return(self.search_result)


    # With the given year and given group name, it will provide the description of the group
    def gen_group_variable_desc(self):
        group = self.table
        try:
            self.groups
        except:
            self.gen_acs_groups(self.year)

        if type(group) == str:
            pattern = re.compile(f'{group}', flags = re.I)
            while self.groups.name.str.match(pattern).sum() == 0:
                group = str(input(f'{group} is not found in the acs5 dataset. \n Please check the name again and provide the correct one:'))
                pattern = re.compile(f'{group}', flags = re.I)
            search = self.variable_table.loc[self.variable_table.name.str.match(pattern),:]
            search = search.sort_values(by = 'name')
        else:
            if bool(iter(group)):
                group_regex = '|'.join(map(lambda x: '(' + x + ')', group))
                pattern = re.compile(group_regex, flags = re.I)
                search = self.variable_table.loc[self.variable_table.name.str.match(pattern),:]
                search = search.sort_values(by = 'name')
            else:
                group = str(input('Please provide the appropriate group keyword, i.e. B28005: '))
                pattern = re.compile(f'{group}', flags = re.I)
                while self.groups.name.str.match(pattern).sum() == 0:
                    group = str(input(f'{group} is not found in the acs5 dataset. \n Please check the name again and provide the correct one:'))
                    pattern = re.compile(f'{group}', flags = re.I)
                search = self.variable_table.loc[self.variable_table.name.str.match(pattern),:]
                search = search.sort_values(by = 'name')

        search = search.loc[search.name.str.match(re.compile(f'{group}_\d+E'))]


        var_desc = search.sort_values('name').reset_index(drop = True)
        if var_desc.source[0] == 'acs/acs5':
            var_desc = var_desc.merge(var_desc.label.str.split('\:\!\!', expand = True),left_index = True, right_index = True).drop(0, axis = 1)
        elif var_desc.source[0] == 'acs/acs5/profile':
            var_desc = var_desc.merge(var_desc.label.str.split('\!\!', expand = True),left_index = True, right_index = True).drop(0, axis = 1)
        N = var_desc.shape[1] -4
        var_desc.columns = var_desc.columns.to_list()[:4] + [f'var_{x}' for x in range(N)]
        del search
        var_desc.loc[var_desc.name.str.match(re.compile(f'{group}_[0]+1E')),f'var_{N-1}'] = 'Total'
        self.group_variable_desc = var_desc
        return var_desc

    def rename_group(self, sub, inplace = False):
        try:
            self.acs_data
        except:
            self.gen_dataframe()
        if type(sub) == dict:
            colname = self.acs_data.columns.to_series()
            for key, values in sub.items():
                pattern = re.compile(values, flags = re.I)
                index = colname.str.match(pattern)
                colname[index] = colname[index].str.replace(values, key)
                if inplace:
                    self.acs_data.columns = colname
                    self.refresh()
                    return(self.acs_data)
                else:
                    data_copy = self.acs_data.copy()
                    data_copy.columns = colname
                    data_copy = data_copy.sort_values('FIPS').reset_index(drop = True)
                    return(data_copy)
        else:
            print('You should provide a dictionary for the sub argument')

    # This drops variables in the acs_data by the group name
    def group_drop(self,  group_name):
        self.acs_data = self.drop(self.acs_data, colname = group_name, group = True)


    def group_isel(self, groupname, variable_suffix, stack = False):
        copy = self.acs_data.copy()
        if type(groupname) == str:
            pattern = re.compile(groupname, flags = re.I)
            colname = self.colname[self.colname.str.match(pattern)].sort_values()
            colname.reset_index(drop = True, inplace = True)
            if type(variable_suffix) != np.ndarray:
                variable_suffix = np.array(variable_suffix)
            variable_suffix = variable_suffix - 1
            column_names = colname[variable_suffix]
            column_names = colname.to_numpy()
            column_names = np.append(self.colname.to_numpy()[:2], column_names)
            table = copy.loc[:, column_names]
        else:
            colname = np.array(self.colname)
            col = colname[:2]
            colname = [self.colname[self.colname.str.match(re.compile(x, flags = re.I))].sort_values().to_numpy() for x in groupname]
            if type(variable_suffix) != np.ndarray:
                variable_suffix = np.array(variable_suffix)
            variable_suffix = variable_suffix - 1
            column_names = [x[y] for x,y in zip(colname, variable_suffix)]
            N = len(column_names)
            for i in range(N):
                col = np.append(col, column_names[i])
            table = copy.loc[:, col]
        if stack:
            stacked_table = table.set_index(list(table.columns[:2])).stack().reset_index()
            stacked_col = stacked_table.columns.to_series()
            stacked_col[2:] = ['Variable', 'Values']
            stacked_table.columns = stacked_col
            return(stacked_table)
        else:
            return(table)




    # This aggregate on a series of variables and
    def aggregate(self, variables_dictionary, aggfunction = np.sum, inplace = False):
        final_column = self.acs_data.columns.to_list()[:2]
        colname = self.colname
        copied_data = self.acs_data.copy()
        for group, sub in variables_dictionary.items():
            for new_name, suffices in sub.items():
                final_column.append(new_name)
                variables = ['(' + group + '_' + x + ')' for x in suffices]
                regex = '|'.join(variables)
                pattern = re.compile(regex, flags = re.I)
                index = colname.str.match(pattern)
                column = colname[index]
                sliced = self.acs_data.copy()
                sliced = sliced.loc[:, column]
                sliced = sliced.astype(float)
                if inplace:
                    self.acs_data[new_name] = sliced.aggregate(func = aggfunction, axis = 1)
                    self.refresh()
                else:
                    copied_data[new_name] = sliced.aggregate(func = aggfunction, axis = 1)
                    copied_data = copied_data.sort_values('FIPS').reset_index( drop = True)
        if inplace:
            return(self.acs_data)
        else:
            return(copied_data.loc[:, final_column])

    def iaggregate(self, variables_dictionary, aggfunction = np.sum, inplace = False):
        self.refresh()
        final_column = self.acs_data.columns.to_numpy()[:2]
        colname =self.colname.sort_values()
        copied_data = self.acs_data.copy().sort_values('FIPS').reset_index(drop = True)
        for group, sub in variables_dictionary.items():
            for new_name, suffices in sub.items():
                final_column = np.append(final_column, f'{new_name}_{group}')
                variables = np.array(suffices) - 1
                index = colname.str.match(re.compile(group, flags = re.I))
                column = colname[index]
                column = column[variables]
                sliced = self.acs_data.copy().sort_values('FIPS').reset_index(drop = True)
                sliced = sliced.loc[:, column]
                sliced = sliced.astype(float)
                if inplace:
                    self.acs_data[f'{new_name}_{group}'] = sliced.aggregate(func = aggfunction, axis = 1)
                    self.refresh()
                else:
                    copied_data[f'{new_name}_{group}'] = sliced.aggregate(func = aggfunction, axis = 1)
                    copied_data = copied_data.sort_values('FIPS').reset_index( drop = True)
        if inplace:
            self.refresh()
            return(self.acs_data)
        else:
            return(copied_data.loc[:, final_column])





    def gen_subgroups(self, new_variables, groups):
        from functools import reduce
        from itertools import product
        self.refresh()
        # First, we need to find the combinations of the new_variables where they contain at least one original variable in the group.
        # For example, the combination of Age under 18 years and No computer in home and broadband access does not have cases because
        # not having a computer excludes the cases where you have the broadband access.
        variables = list(new_variables.keys())
        subgroups = [list(x.keys()) for x in list(new_variables.values())]
        subindex = [list(x.values()) for x in list(new_variables.values())]
        comb_subgroups = np.array(list(product(*subgroups)))
        subgroups_index = list(map(lambda x: reduce(np.intersect1d, x), list(product(*subindex))))
        index_size = np.array(list(map(lambda x: x.shape[0], subgroups_index)))
        comb_subgroups = comb_subgroups[np.not_equal(index_size, 0)]
        subgroups_index = list(map(lambda x : subgroups_index[x], (np.arange(len(subgroups_index))[np.not_equal(index_size, 0)])))
        subgroups_index = [x.tolist() for x in subgroups_index]
        copy = self.acs_data.copy().sort_values('FIPS')
        frame = pd.DataFrame(comb_subgroups, columns = variables)
        n = frame.shape[0]
        p = frame.shape[1]
        N = copy.shape[0]
        frame = pd.concat([frame]*N, ignore_index = True)
        index = np.repeat(copy.FIPS, n)
        frame.index = index
        frame = frame.merge(copy, how = 'left', left_on = 'FIPS', right_on = 'FIPS').iloc[:, [0, p+1, p+2 ] + list(range(1, p+1))]
        comb_subgroups = list(map(lambda x: ' & '.join(x), comb_subgroups))
        arg = dict(zip(comb_subgroups, subgroups_index))
        arg1 = {}
        if type(groups) == str:
            arg1[groups] = arg
        else:
            for i in groups:
                arg1[i]= arg
        self.temp = self.iaggregate(arg1)
        temp = self.temp.iloc[:,2:]
        k = int(temp.shape[1]/n)
        for i in range(int(k)):
            column_index = range(i*n, (i+1)*n)
            source = temp.iloc[:, column_index]
            frame[groups[i]] = source.to_numpy().reshape(-1)
        frame = frame.reset_index(drop = True)
        return(frame)


    def merge_gen_subgroups(self, list_new_variables, list_groups):
        output = self.gen_subgroups(list_new_variables[0], list_groups[0])
        new_variables = list_new_variables[0]
        # I just need to find n
        variables = list(new_variables.keys())
        n = len(variables)
        for i in range(1, len(list_new_variables)):
            new_output = self.gen_subgroups(list_new_variables[i], list_groups[i])
            output = output.merge(new_output, on = list(output.columns[:3+n]))
        return(output)



    def rename_columns(self, var_name = None):
        var_desc = self.gen_group_variable_desc()
        if var_desc.shape[1] == 5:
            var_desc.dropna(inplace= True)
            rename_dict = dict(zip(var_desc.name, var_desc.var_0))
            self.acs_data = self.acs_data.rename(columns = rename_dict)
        if var_name == None:
            pass

    def aggregate_by_var_name(self, var_name):
        pass

##############################################################################
##############################################################################
##############################################################################
##############################################################################
##############################################################################

class census_sdoh(acs):
    def __init__(self, year = 2020, state = 21, region = 'Tract', run_query = False):

        self.ten_years = ['Under 5 years', '5 to 14 years','15 to 24 years',
                              '25 to 34 years','35 to 44 years','45 to 54 years',
                              '55 to 64 years','65 to 74 years','75 to 84 years',
                              '85 years and over']
        self.total_years = ['Under 18', '18 to 64', 'Over 64']
        super().__init__()
        self.year = year
        self.state = state
        self.region = region
        self.source = 'acs/acs5'
        self.sdoh_groups = {'demo_all': 'B01001', 'demo_White':'B01001H', 'demo_Black':'B01001B',
                       'demo_Hispanic': 'B01001I', 'demo_Asian': 'B01001D', 'education':'B15003','employment':'B23025',
                       'gini_index': 'B19083', 'rent_to_income':'B25070', 'tenure': 'B25008',
                       'vacancy':'B25002','years_built':'B25034', 'median_income_all': 'B19013',
                       'median_income_white': 'B19013H', 'median_income_black': 'B19013B',
                       'median_income_hispanic': 'B19013I','insurance':'B27001',
                       'transportation_means': 'B08141', 'poverty': 'B17026',
                       'computer':'B28003', 'internet': 'B28011', 'public_assistance':'B19058',
                       'medicaid':'C27007'
                       }
        self.urban_rural()
        self.sdoh_df = {}
        if run_query:
            import multiprocessing as mp
            #import threading
            p1 = mp.Process(target = self.demographic_table())
            p2 = mp.Process(target = self.technology_table())
            p3 = mp.Process(target = self.employment_table())
            p4 = mp.Process(target = self.gini_index_table())
            p5 = mp.Process(target = self.rent_to_income_table())
            p6 = mp.Process(target = self.vacancy_table())
            p7 = mp.Process(target = self.incomes_table())
            p8 = mp.Process(target = self.insurance_table())
            p9 = mp.Process(target = self.old_house_table())
            p10 = mp.Process(target = self.transportation_table())
            p11 = mp.Process(target = self.poverty_table())
            p12 = mp.Process(target = self.education_table())
            p13 = mp.Process(target = self.public_assistance_table())

            p1.start(); p2.start(); p3.start(); p4.start(); p5.start(); p6.start()
            p7.start(); p8.start(); p9.start(); p10.start(); p11.start(); p12.start(); p13.start()
            p1.join();  p2.join();  p3.join();  p4.join();  p5.join();  p6.join()
            p7.join();  p8.join(); p9.join(); p10.join(); p11.join(); p12.join(); p13.join()

            if self.region.upper() == 'COUNTY':
                self.urban_rural_counties = self.urban_rural_counties.merge(self.sdoh_df['education'])
                self.urban_rural_counties = self.urban_rural_counties[['FIPS','County','State','Urban_Percentage']]
                self.sdoh_df['urban_rural'] = self.urban_rural_counties

    def urban_rural(self):
        self.urban_rural_counties = pd.read_excel('https://www2.census.gov/geo/docs/reference/ua/PctUrbanRural_County.xls')
        self.urban_rural_counties = self.urban_rural_counties.loc[self.urban_rural_counties.STATE.eq(int(self.state)),['STATE','COUNTY','STATENAME','COUNTYNAME','POPPCT_URBAN']]
        county_fips = self.urban_rural_counties.COUNTY.astype(str)
        state_fips = self.urban_rural_counties.STATE.astype(str)
        county_fips = county_fips.apply(lambda x: '0'+x if len(x) < 3 else x)
        county_fips = county_fips.apply(lambda x: '0'+x if len(x) < 3 else x)
        self.urban_rural_counties['FIPS'] = [str(x)+str(y) for x,y in zip(state_fips, county_fips)]
        self.urban_rural_counties = self.urban_rural_counties[['FIPS','COUNTYNAME','STATENAME','POPPCT_URBAN']]
        self.urban_rural_counties.rename(columns = {'COUNTYNAME': 'County','STATENAME':'State','POPPCT_URBAN':'Urban_Percentage'}, inplace = True)
        self.urban_rural_counties['Urban_Percentage'] = self.urban_rural_counties.Urban_Percentage * 0.01
        self.urban_rural_counties['FIPS'] = self.urban_rural_counties.FIPS.astype(str)
        self.urban_rural_counties.reset_index(drop = True, inplace = True)
        self.urban_rural_counties['County'] = self.urban_rural_counties.County + ' County'
        self.urban_rural_counties.drop('State', axis = 1, inplace = True)


    def merge_sdoh_df(self):
        first = list(self.sdoh_df.keys())[0]
        for keys, df in self.sdoh_df.items():
            if keys == first:
                output = df
            else:
                if self.region == 'tract':
                    output = output.merge(df, on = ['FIPS','Tract','County', 'State'], how = 'inner')
                elif self.region == 'county':
                    output = output.merge(df, on = ['FIPS','County', 'State'], how = 'inner')
        self.sdoh_table_single = output

    def demographic_table(self):
        self.table = self.sdoh_groups['demo_all']
        self.validate_attributes()
        df = self.gen_dataframe(return_table = True)
        var_desc = self.gen_group_variable_desc()
        colnames = var_desc.var_1.unique()[2:]

        total_index = [0, 4, 17, 23]
        total_df = self.gen_dataframe(return_table = True)
        total_df['Total'] = df[self.sdoh_groups['demo_all'] + '_001E'].astype(int)
        for i, ind in enumerate(total_index):
            if i < len(total_index) - 1:
                total_df[self.total_years[i]] = total_df.loc[:, var_desc.loc[var_desc.var_1.isin(colnames[ind:total_index[i+1]]),'name'].values].astype(int).sum(axis =1)
        for colname in self.total_years:
            total_df[colname] = total_df[colname]/total_df.Total
        self.sdoh_df['demo_total'] =  total_df.loc[:, ~total_df.columns.str.match(re.compile(self.sdoh_groups['demo_all']))]
        index = [0, 1, 3, 8, 10, 12, 14, 17, 20, 22, 23]
        for i, ind in enumerate(index):
            if i < len(index)-1:
                df[self.ten_years[i]] = df.loc[:, var_desc.loc[
                    var_desc.var_1.isin(colnames[ind: index[i+1]]),
                    'name'].values].sum(axis = 1)
            else:
                pass
        self.sdoh_df['demo_all'] = df.loc[:, ~df.columns.str.match(re.compile(self.sdoh_groups['demo_all']))]

        # for other races
        self.table = 'B03002'
        self.validate_attributes()
        df = self.gen_dataframe(return_table = True)
        frame = df.loc[:,['FIPS','County','State']]
        var_desc = self.gen_group_variable_desc()
        white = df.B03002_003E.astype(int)/df.B03002_001E.astype(int)
        white.name = 'total'
        self.sdoh_df['demo_White'] = pd.concat([frame, white], axis = 1)

        black = df.B03002_004E.astype(int)/df.B03002_001E.astype(int)
        black.name = 'total'
        self.sdoh_df['demo_Black'] = pd.concat([frame, black], axis = 1)


        hispanic = df.B03002_012E.astype(int)/df.B03002_001E.astype(int)
        hispanic.name = 'total'
        self.sdoh_df['demo_Hispanic'] = pd.concat([frame, hispanic], axis = 1)

        asian = df.B03002_006E.astype(int)/df.B03002_001E.astype(int)
        asian.name = 'total'
        self.sdoh_df['demo_Asian'] = pd.concat([frame, asian], axis = 1)

        other_races = (df.B03002_002E.astype(int) - df.loc[:,['B03002_003E','B03002_004E','B03002_006E']].astype(int).sum(1))/df.B03002_001E.astype(int)
        other_races.name = 'total'
        self.sdoh_df['demo_Other_Races'] = pd.concat([frame, other_races], axis = 1)


    def education_table(self):
        self.table= self.sdoh_groups['education']
        self.validate_attributes()
        df = self.gen_dataframe(return_table = True)
        var_desc = self.gen_group_variable_desc()
        # col1 is for below 9th grade
        col1 = ['Nursery school', 'No schooling completed', '5th grade', '3rd grade', '4th grade', '2nd grade', '1st grade', 'Kindergarten','8th grade', '7th grade', '6th grade']
        # col4 is for advanced degree
        col4 = ['Doctorate degree','Professional school degree', "Master's degree"]
        # col3 is for 4 years college and above: (it changes at the end, but for now, it includes any college to define col2, which is high school and above)
        col3 = ["Bachelor's degree", "Associate's degree", "Some college, 1 or more years, no degree",'Some college, less than 1 year'] + col4
        # col2 is high school and above
        col2 = ['Regular high school diploma'] + col3 + col4
        # col5 is for completed college
        col5 = ["Bachelor's degree"] + col4


        df['Total'] = df.B15003_001E.astype(float)
        df['Below 9th grade'] = df.loc[:,var_desc.loc[var_desc.var_0.isin(col1), 'name']].sum(axis = 1).astype(float)/df.Total
        df['High School'] = df.loc[:,var_desc.loc[var_desc.var_0.isin(col2), 'name']].sum(axis = 1).astype(float)/df.Total
        df['College'] = df.loc[:,var_desc.loc[var_desc.var_0.isin(col5), 'name']].sum(axis = 1).astype(float)/df.Total
        df['Advanced Degree'] = df.loc[:,var_desc.loc[var_desc.var_0.isin(col4), 'name']].sum(axis = 1).astype(float)/df.Total
        df.drop('Total',axis = 1, inplace = True)
        df = df.loc[:, ~df.columns.str.match(re.compile(self.table))]
        self.sdoh_df['education'] = df



    def employment_table(self):
        self.table= self.sdoh_groups['employment']
        self.validate_attributes()
        df = self.gen_dataframe(return_table = True)
        df['Labor Force Participation Rate'] = df.B23025_003E/(df.B23025_003E + df.B23025_007E)
        df['Unemployment Rate'] = df.B23025_005E/df.B23025_003E
        self.sdoh_df['employment'] = df.loc[:, ~df.columns.str.match(re.compile(self.table))]


    def gini_index_table(self):
        self.table = self.sdoh_groups['gini_index']
        self.validate_attributes()
        df = self.gen_dataframe(return_table = True)
        df = df.rename(columns = {f'{self.table}_001E': 'Gini Index'})
        df['Gini Index'] = df['Gini Index'].astype(float).apply(lambda x: x if x>=0 else None)
        self.sdoh_df['gini_index'] = df


    def rent_to_income_table(self):
        self.table = self.sdoh_groups['rent_to_income']
        self.validate_attributes()
        df = self.gen_dataframe(return_table = True)
        df['rent_over_40'] = (df.B25070_009E + df.B25070_010E).astype(float)/df.B25070_001E.astype(float)
        self.sdoh_df['rent_to_income'] = df.loc[:, ~df.columns.str.match(re.compile(self.table))]



    def vacancy_table(self):
        self.table = self.sdoh_groups['vacancy']
        self.validate_attributes()
        df = self.gen_dataframe(return_table = True)
        df['vacancy_rate'] = df.B25002_003E.astype(float)/df.B25002_001E.astype(float)
        self.sdoh_df['vacancy'] = df.loc[:, ~df.columns.str.match(re.compile(self.table))]


    def insurance_table(self):
        self.table = self.sdoh_groups['insurance']
        self.validate_attributes()
        df = self.gen_dataframe(return_table = True)
        var_desc = self.gen_group_variable_desc()
        df['health_insurance_coverage_rate'] = df.loc[:, var_desc.loc[var_desc.var_2.eq('With health insurance coverage'), 'name']].sum(axis = 1).astype(float)/df.B27001_001E.astype(float)
        df = df.loc[:, ~df.columns.str.match(re.compile(self.table))]
        self.table = self.sdoh_groups['medicaid']
        self.validate_attributes()
        var_desc = self.gen_group_variable_desc()
        df1 = self.gen_dataframe(return_table = True)
        var_desc = self.gen_group_variable_desc()
        df1['medicaid'] = df1.loc[:,var_desc.loc[var_desc.var_2.eq('With Medicaid/means-tested public coverage'),'name']].sum(axis = 1)/df1.C27007_001E.astype(float)
        df1 = df1.loc[:, ~df1.columns.str.match(re.compile(self.table))]
        df = df.merge(df1)
        self.sdoh_df['insurance'] = df


    def incomes_table(self):
        r = 'all'

        self.table = self.sdoh_groups[f'median_income_{r}']
        self.validate_attributes()
        df = self.gen_dataframe(return_table = True)
        df[f'median_income_{r}'] = df[f'{self.table}_001E'].astype(float)
        df.loc[df[f'median_income_{r}'].le(0), f'median_income_{r}'] = np.nan
        df = df.loc[:, ~df.columns.str.match(re.compile(self.table))]
        output = df
        self.sdoh_df['income'] = output


    def old_house_table(self):
        self.table = self.sdoh_groups['years_built']
        self.validate_attributes()
        df = self.gen_dataframe(return_table = True)
        df['houses_before_1960'] = df[['B25034_009E','B25034_010E','B25034_011E']].sum(axis = 1)
        df = df.loc[:, ~df.columns.str.match(re.compile(self.table))]
        self.sdoh_df['houses_before_1960'] = df



    def public_assistance_table(self):
        self.table = self.sdoh_groups['public_assistance']
        self.validate_attributes()
        df = self.gen_dataframe(return_table = True)
        df['public_assistance_recieved'] = df.B19058_002E.astype(float)/df.B19058_001E.astype(float)
        df = df.loc[:, ~df.columns.str.match(re.compile(self.table))]
        self.sdoh_df['public_assistance'] = df


    def technology_table(self):
        self.table = self.sdoh_groups['computer']
        self.validate_attributes()
        df = self.gen_dataframe(return_table = True)
        computer = df.B28003_002E.astype(float)/df.B28003_001E.astype(float)
        self.table = self.sdoh_groups['internet']
        self.validate_attributes()
        df = self.gen_dataframe(return_table = True)
        df['computer'] = computer
        df[['B28011_008E','B28011_004E','B28011_001E']] = df.loc[:,['B28011_008E','B28011_004E','B28011_001E']].astype(float)
        df['internet_acess']  = 1 - (df.B28011_008E / df.B28011_001E)
        df['broadband_subscription'] = (df.B28011_004E / df.B28011_001E)
        df = df.loc[:, ~df.columns.str.match(re.compile(self.table))]
        self.sdoh_df['technology'] = df



    def poverty_table(self):
        self.table = self.sdoh_groups['poverty']
        self.validate_attributes()
        df = self.gen_dataframe(return_table = True)
        df['below_poverty_x.5'] = df.B17026_002E.astype(float)/df.B17026_001E.astype(float)
        df['below_poverty'] = df.loc[:, df.columns.str.match(re.compile('B17026_00[2-4]E'))].sum(axis = 1).astype(float)/df.B17026_001E.astype(float)
        df['below_poverty_x2'] = (df.B17026_010E + df.loc[:, df.columns.str.match(re.compile('B17026_00[2-9]E'))].sum(axis = 1)).astype(float)/df.B17026_001E.astype(float)
        df = df.loc[:, ~df.columns.str.match(re.compile(self.table))]
        self.sdoh_df['poverty'] = df


    def transportation_table(self):
        self.table = self.sdoh_groups['transportation_means']
        self.validate_attributes()
        df = self.gen_dataframe(return_table = True)
        df['no_vehicle'] = df.B08141_002E.astype(float)/df.B08141_001E.astype(float)
        df['two_or_more_vehicle']  = (df.B08141_004E + df.B08141_005E).astype(float)/df.B08141_001E.astype(float)
        df['three_or_more_vehicle'] = df.B08141_005E.astype(float)/df.B08141_001E.astype(float)
        df = df.loc[:, ~df.columns.str.match(re.compile(self.table))]
        self.sdoh_df['transportation'] = df

##############################################################################
##############################################################################
##############################################################################
##############################################################################
##############################################################################

class facilities:
    def __init__(self):
        pass

    def mammography(self, state = 'KY'):

        url = urllib.request.urlopen("http://www.accessdata.fda.gov/premarket/ftparea/public.zip")

        with ZipFile(BytesIO(url.read())) as my_zip_file:
            df = pd.DataFrame(my_zip_file.open(my_zip_file.namelist()[0]).readlines(), columns= ['main'])
            df = df.main.astype(str).str.split('|', expand = True)
            df.columns = ['Name','Street','Street2','Street3','City','State','Zip_code','Phone_number', 'Fax']
            # state = state.upper()
            df = df.loc[df.State.isin(state)].reset_index(drop = True)
            df.Name = df.Name.str.extract(re.compile('[bB].(.*)'))
            df['Address'] = df['Street'] + ', ' + df['City'] + ', ' +  df['State'] + ' ' + df['Zip_code']
            df['Type'] = 'Mammography'
            df['Notes'] = ''
        self.mammography_df = df.loc[:,['Type','Name','Address','Phone_number', 'Notes']] #try to add FIPS and State

    def hpsa(self, location = 'KY'):
        df= pd.read_excel('https://data.hrsa.gov/DataDownload/DD_Files/BCD_HPSA_FCT_DET_PC.xlsx')
        df.columns = df.columns.str.replace(' ','_')
        df = df.loc[df.Common_State_County_FIPS_Code.isin(location)&
                    df.HPSA_Status.eq('Designated')&
                    df.Designation_Type.ne('Federally Qualified Health Center')].reset_index(drop = True)
        df = df[['HPSA_Name','HPSA_ID','Designation_Type','HPSA_Score','HPSA_Address',
                 'HPSA_City', 'State_Abbreviation', 'Common_State_County_FIPS_Code',
                 'HPSA_Postal_Code','Longitude','Latitude']]
        pattern = re.compile('(\d+)-\d+')
        df['HPSA_Postal_Code']  = df.HPSA_Postal_Code.str.extract(pattern)
        df['HPSA_Street'] = df['HPSA_Address'] + ', ' + df['HPSA_City'] + \
            ', ' + df['State_Abbreviation'] + ' ' + df['HPSA_Postal_Code']
        df = df.drop_duplicates()
        df['Type'] = 'HPSA '+df.Designation_Type
        df = df.rename(columns = {'HPSA_Name' : 'Name', 'HPSA_Street':'Address',
                                  'Common_State_County_FIPS_Code': 'FIPS', 'State_Abbreviation': 'State'})
        df = df[['Type','Name','HPSA_ID','Designation_Type','HPSA_Score','Address',
                 'FIPS', 'State', 'Longitude','Latitude']]
        df = df.loc[df.Longitude.notnull()|df.Address.notnull()].reset_index(drop = True)
        df['Phone_number'] = np.nan
        df['Notes'] = ''
        self.hpsa_full_df = df
        self.hpsa_df = df[['Type','Name','Address', 'Phone_number', 'Notes']] #try to add FIPS and State

    def fqhc(self, location = 'KY'):
        df= pd.read_csv('https://data.hrsa.gov//DataDownload/DD_Files/Health_Center_Service_Delivery_and_LookAlike_Sites.csv')
        df.columns = df.columns.str.replace(' ','_')
        df = df.loc[df.State_and_County_Federal_Information_Processing_Standard_Code.isin(location)&
            df.Health_Center_Type.eq('Federally Qualified Health Center (FQHC)')&
            df.Site_Status_Description.eq('Active')].reset_index(drop = True)
        df = df[['Health_Center_Type', 'Site_Name','Site_Address','Site_City','Site_State_Abbreviation',
                 'Site_Postal_Code','Site_Telephone_Number',
                 'Health_Center_Service_Delivery_Site_Location_Setting_Description',
                 'Geocoding_Artifact_Address_Primary_X_Coordinate',
                 'Geocoding_Artifact_Address_Primary_Y_Coordinate']]
        df['Type'] = 'FQHC'
        df['Address'] = df['Site_Address'] + ', ' + df['Site_City'] + ', ' + \
                        df['Site_State_Abbreviation'] + ' ' + df['Site_Postal_Code']
        df = df.rename(columns = {'Site_Name':'Name', 'Site_Telephone_Number': 'Phone_number',
                                  'State_Abbreviation': 'State',
                                  'Health_Center_Service_Delivery_Site_Location_Setting_Description': 'Notes',
                                  'Geocoding_Artifact_Address_Primary_X_Coordinate': 'Longitude',
                                  'Geocoding_Artifact_Address_Primary_Y_Coordinate': 'Latitude'})
        df = df.loc[df.Address.notnull()].reset_index(drop = True)
        self.fqhc_df = df[['Type', 'Name', 'Address', 'Phone_number', 'Notes']]

    def nppes(self, location  = 'KY'):
        taxonomy= ['Gastroenterology','colon','obstetrics']

        count = 0
        for lct in location:
            for taxonomy_description in taxonomy:
                limit = 200
                skip = 0


                nrow = 200
                while nrow == 200:
                    api_call = f'https://npiregistry.cms.hhs.gov/api/?version=2.1&address_purpose=LOCATION&number=&state={lct}&taxonomy_description={taxonomy_description}&skip={skip}&limit={limit}'
                    response = requests.get(api_call)
                    if bool(response):
                        if count ==0:
                            global df
                            df = pd.DataFrame(response.json()['results'])
                            df = df[['enumeration_type', 'addresses','basic']]
                            if taxonomy_description == 'colon':
                                df['taxonomies'] = 'Colon & Rectal Surgeon'
                            elif taxonomy_description ==  'obstetrics':
                                df['taxonomies'] = 'Obstetrics & Gynecology'
                            else:
                                df['taxonomies'] = taxonomy_description
                            df['street'] = [x[0]['address_1'] +', ' + x[0]['city'] +', ' + x[0]['state']+ ' ' + x[0]['postal_code'][:5] for x in df.addresses]
                            for x in df.addresses:
                                if 'telephone_number' in x[0].keys():
                                    x[0]['phone_number'] = x[0]['telephone_number']
                                else:
                                    x[0]['phone_number'] = 'NA'
                            df['phone_number'] = [x[0]['phone_number'] for x in df.addresses]
                            for x in df.basic:
                                if 'first_name' in x.keys():
                                    x['name'] = x['first_name'] + ' ' + x['last_name']
                                else:
                                    x['name'] = x['organization_name']
                            df['name'] = [x['name'] for x in df.basic]
                            df.drop(['addresses','basic'], axis = 1, inplace = True)
                            df = df[['taxonomies','name','phone_number','street']]
                            skip += 200
                            nrow = df.shape[0]
                            count += 1
                        else:
                            df1 = pd.DataFrame(response.json()['results'])
                            df1 = df1[['enumeration_type', 'addresses','basic']]
                            if taxonomy_description == 'colon':
                                df1['taxonomies'] = 'Colon & Rectal Surgeon'
                            elif taxonomy_description ==  'obstetrics':
                                df1['taxonomies'] = 'Obstetrics & Gynecology'
                            else:
                                df1['taxonomies'] = taxonomy_description
                            df1['street'] = [x[0]['address_1'] +', ' + x[0]['city'] +', ' + x[0]['state']+ ' ' + x[0]['postal_code'][:5] for x in df1.addresses]
                            for x in df1.addresses:
                                if 'telephone_number' in x[0].keys():
                                    x[0]['phone_number'] = x[0]['telephone_number']
                                else:
                                    x[0]['phone_number'] = 'NA'
                            df1['phone_number'] = [x[0]['phone_number'] for x in df1.addresses]
                            for x in df1.basic:
                                if 'first_name' in x.keys():
                                    x['name'] = x['first_name'] + ' ' + x['last_name']
                                else:
                                    x['name'] = x['organization_name']
                            df1['name'] = [x['name'] for x in df1.basic]
                            df1.drop(['addresses', 'basic'], axis = 1, inplace = True)
                            df1 = df1[['taxonomies','name','phone_number','street']]
                            df = pd.concat([df, df1], ignore_index = True)
                            skip += 200
                            nrow = df.shape[0]
                            count += 1

        df['Type'] = df.taxonomies
        df['Notes'] = ''
        df = df.rename(columns = {'name': 'Name', 'phone_number' : 'Phone_number','street':'Address'})
        self.nppes_df = df[['Type','Name','Address','Phone_number', 'Notes']]

    def return_lung_cancer_screening_data(self):
        return self.lung_cancer_screening_df

    def lung_cancer_screening(self, chrome_driver_path = None, download_path = None, location = 'KY', remove_download = False):
        # from selenium.webdriver.support.ui import WebDriverWait as wait
        # from selenium.webdriver.common.keys import Keys
        # from selenium.webdriver.support.ui import Select
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        import os
        import time

        if chrome_driver_path == None:
            chrome_driver_path = 'chromedriver'
            if sys.platform.lower()[:3] == 'win':
            	chrome_driver_path += '.exe'
            chrome_driver_path = os.path.join(os.getcwd() , 'input_folder', chrome_driver_path) # zella note: 'input_folder' added for testing
        try:
            if download_path:
                chrome_options = webdriver.ChromeOptions()
                prefs = {'download.default_directory' : download_path}
                chrome_options.add_experimental_option('prefs', prefs)
                driver = webdriver.Chrome(chrome_driver_path, chrome_options = chrome_options)
            else:
                driver = webdriver.Chrome(chrome_driver_path)

            url = 'https://report.acr.org/t/PUBLIC/views/NRDRLCSLocator/LCSLocator?:embed=y&:showVizHome=no&:host_url=https%3A%2F%2Freport.acr.org%2F&:embed_code_version=3&:tabs=no&:toolbar=no&:showAppBanner=no&:display_spinner=no&:loadOrderID=0'
            driver.get(url)
            time.sleep(10)
            state = driver.find_elements(By.CLASS_NAME, 'tabComboBoxButtonHolder')[2]
            state.click()
            time.sleep(10)
            state2 = driver.find_elements(By.CLASS_NAME, 'tabMenuItemNameArea')[1]
            state2.click()
            time.sleep(10)
            if download_path == None:
                download = driver.find_element(By.ID, 'tabZoneId422')
                download.click()
                print('check your download folder for the file')
                driver.close()
            else:
                download = driver.find_element(By.ID, 'tabZoneId422')
                download.click()
                t = 0

                while t == False:
                    time.sleep(5)
                    t = os.path.isfile(f'{download_path}/ACRLCSDownload.csv')
                    print('Waiting on LCSR data...')
                else:
                    print('LCSR data ready')

                driver.close()
                file_path = os.path.join(download_path, 'ACRLCSDownload.csv')
                print(file_path)
                df = pd.read_csv(file_path)
                print('reading data is complete')
                df.columns = ['Name','Street','City','State','Zip_code','Phone','Designation','site_id','facility_id','Registry Participant']
                df['Address'] = df['Street'] + ', ' + df['City'] + ', ' +  df['State'] + ' ' + df['Zip_code']
                df['Type'] = 'Lung Cancer Screening'
                df['Phone_number'] = df['Phone']
                df['Notes'] = ''
                df = df.loc[df.State.isin(location)]
                df = df[['Type','Name', 'Address', 'Phone_number', 'Notes']]
                self.lung_cancer_screening_df = df.copy()
                if remove_download:
                    os.remove(f'{download_path}/ACRLCSDownload.csv')
        except:
            print(f'please locate the chrome driver file in {chrome_driver_path} (TEST 2)') # Zella note: adding test text to distinguish
            print('you can download the driver file from https://sites.google.com/chromium.org/driver/ (TEST 2)') # Zella note: adding test text to distinguish




##############################################################################
##############################################################################
##############################################################################
##############################################################################
##############################################################################

# class for employment data
# will return monthly unemployment rates

class BLS:
    def __init__(self, state = '21', most_recent = True):
        response = requests.get('https://www.bls.gov/web/metro/laucntycur14.txt')
        df = pd.DataFrame([x.strip().split('|') for x in response.text.split('\n')[6:-7]],
                          columns = ['LAUS Area Code','State','County','Area',
                                     'Period','Civilian Labor Force','Employed',
                                     'Unemployed','Unemployment Rate'] )
        df['State'] = df.State.str.strip().astype(str)
        df['County'] = df.County.str.strip().astype(str)
        df['County'] = ['0'+x if len(x) < 3 else x for x in df.County]
        df['County'] = ['0'+x if len(x) < 3 else x for x in df.County]
        # df['Employed'] = df.Employed.str.strip().str.replace(',','').astype(float)
        df['Employed'] = df.Employed.replace(r'-', np.NaN, regex=True).replace(',','', regex=True).astype(float) # ZELLA CHANGE: replaced this line
        df['Unemployed'] = df.Unemployed.replace(r'-', np.NaN, regex=True).replace(',','', regex=True).astype(float)
        df['Unemployment Rate'] = df['Unemployment Rate'].replace(r'-', np.NaN, regex=True).replace(',','', regex=True).astype(float)
        df['FIPS'] = df['State']+df['County']
        df['Period'] = df.Period.str.strip()
        if most_recent:
            # df = df.loc[df.Period.str.match(re.compile('.*p\)$'))] # zella note: GIVING AN ERROR
            # ## ERROR: ValueError: Cannot mask with non-boolean array containing NA / NaN values
            # df['Period'] = [x[:-3] for x in df.Period]
            #### ZELLA CHANGE (commented-out lines were giving errors) ####
            df = df.loc[df.Period.str.match(re.compile(r'.*p\)$'), na = False)]
            df['Period'] = [x[:-3] for x in df.Period if type(x) == str]
            #### END ZELLA CHANGE ####
        self.df = df.loc[df.State.eq(state),['FIPS','Unemployment Rate', 'Period']].sort_values('FIPS').reset_index(drop = True)

##############################################################################
##############################################################################
##############################################################################
##############################################################################
##############################################################################

# class for pulling safe drinking water data
# will return count of health-based safe drinking water violations in counties of catchment area

class water_violation(acs):
    def __init__(self, state = 'KY', start_year = 2016):
        super().__init__()

        url_violation = f'https://data.epa.gov/efservice/VIOLATION/IS_HEALTH_BASED_IND/Y/PRIMACY_AGENCY_CODE/{state}/CSV'
        self.violation = pd.read_csv(url_violation)
        self.violation.columns = self.violation.columns.str.replace(re.compile('.*\.'),"")
        self.violation = self.violation.loc[self.violation.COMPL_PER_BEGIN_DATE.notnull() ,:]
        self.violation['date'] = pd.to_datetime(self.violation.COMPL_PER_BEGIN_DATE)
        self.violation = self.violation.loc[self.violation.date.dt.year >= start_year, :].reset_index(drop = True)
        self.violation['indicator'] = 1

        url_systems = f'https://data.epa.gov/efservice/GEOGRAPHIC_AREA/PWSID/BEGINNING/{state}/CSV'
        self.profile = pd.read_csv(url_systems)
        if len(self.profile.index) == 10001:
            url_systems2 = f'https://data.epa.gov/efservice/GEOGRAPHIC_AREA/PWSID/BEGINNING/{state}/rows/10001:20000/CSV'
            self.profile2 = pd.read_csv(url_systems2)
            self.profile = pd.concat([self.profile, self.profile2]).reset_index(drop=True)
        self.profile.columns = self.profile.columns.str.replace(re.compile('.*\.'),"")
        self.profile = self.profile.loc[self.profile['PWS_TYPE_CODE'] == 'CWS']
        self.profile = self.profile.assign(COUNTY_SERVED = self.profile.COUNTY_SERVED.str.split(',')).explode('COUNTY_SERVED')

        self.summarize_water_violations()
        self.df = self.result
        self.df.loc[self.df.counts.isnull(),'counts'] = 0

    def summarize_water_violations(self, state= 'KY'):
        violation = self.violation
        profile = self.profile
        violation_by_pws = violation[['PWSID','VIOLATION_ID','indicator']].groupby(['PWSID','VIOLATION_ID'], as_index = False).max().loc[:,['PWSID','indicator']].groupby('PWSID', as_index = False).sum()
        violation_by_pws.columns = ['PWSID','counts']

        df = profile.merge(violation_by_pws, on = 'PWSID', how='left')
        df = df[['COUNTY_SERVED', 'PRIMACY_AGENCY_CODE', 'counts']].groupby('COUNTY_SERVED', as_index = False).max()
        self.testing = df
        df['County'] = df.COUNTY_SERVED.astype(str) + ' County'
        df['StateAbbrev'] = df.PRIMACY_AGENCY_CODE.astype(str)
        df.drop(['COUNTY_SERVED', 'PRIMACY_AGENCY_CODE'], axis = 1, inplace  = True)
        self.result = df

##############################################################################
##############################################################################
##############################################################################
##############################################################################
##############################################################################

# class for pulling broadband speeds
# will return max upload and download speeds advertized in Census block groups within catchment area

class fcc:
    def __init__(self, state = 'KY', chrome_driver_path = None, download_path = None,  remove_download = False):
        response = requests.get('https://www.fcc.gov/general/broadband-deployment-data-fcc-form-477')
        soup = BeautifulSoup(response.content, "html.parser")
        self.data_url = soup.find_all(href=re.compile(state))[-1]['href']
        self.download_data(state2 = state, chrome_driver_path = chrome_driver_path, download_path = download_path)
        self.gen_df()
        if remove_download: # zella note: commented out for testing
            os.remove(self.file_path)



    def download_data(self, state2 = 'KY', chrome_driver_path = None, download_path = None):
        # from selenium.webdriver.support.ui import WebDriverWait as wait
        # from selenium.webdriver.common.keys import Keys
        # from selenium.webdriver.support.ui import Select
        from selenium import webdriver
        from selenium.webdriver.common.by import By

        if chrome_driver_path == None:
            chrome_driver_path = 'chromedriver'
            if sys.platform.lower()[:3] == 'win':
            	chrome_driver_path += '.exe'
            chrome_driver_path = os.path.join(os.getcwd() , 'input_folder', chrome_driver_path) # zella note: added 'input_folder' for testing only, since it seems like otherwise it might not look in the right place?
        try:
            driver = webdriver.Chrome(chrome_driver_path) # Zella note: seems like this line may be causing errors
            driver.close()
        except:
            print(f'please locate the chrome driver file in {chrome_driver_path} (TEST 1)') # Zella note: adding test text to distinguish NOTE: this is being triggered
            print('you can download the driver file from https://sites.google.com/chromium.org/driver/ (TEST 1)')  # Zella note: adding test text to distinguish
        else:
            if download_path:
                chrome_options = webdriver.ChromeOptions()
                prefs = {'download.default_directory' : download_path}
                chrome_options.add_experimental_option('prefs', prefs)
                driver = webdriver.Chrome(chrome_driver_path, chrome_options = chrome_options)
            else:
                driver = webdriver.Chrome(chrome_driver_path)
            driver.get(self.data_url)
            time.sleep(5)
            driver.find_elements(By.CLASS_NAME, 'btn')[1].click()

            t = 0

            while t == False:
                time.sleep(5)
                t = os.path.isfile(f'{download_path}\\{state2}-Fixed-Jun2021.zip')
                t = len(glob.glob(os.path.join(download_path, '*.zip'), recursive = True)) > 0
                print(t)
                print(f'Waiting on {state2} broadband data...')
            else:
                print(f'{state2} broadband data ready')

            driver.close()

            if download_path == None:
                print('check your download folder for the file')
            else:
                file_path = glob.glob(os.path.join(download_path, '*.zip'), recursive = True)[-1]
                self.file_path = file_path
                print(file_path)

    def gen_df(self):
        file_path = self.file_path # ZELLA NOTE: AttributeError: 'fcc' object has no attribute 'file_path'
        with ZipFile(file_path) as my_zip_file:
            df = pd.read_csv(my_zip_file.open(my_zip_file.namelist()[0]))
            df = df[['StateAbbr','BlockCode','MaxAdDown','MaxAdUp']]
            self.fcc_data = df
            my_zip_file.close()


##############################################################################
##############################################################################
##############################################################################
##############################################################################
##############################################################################

# class for pulling food deserts
# will return Census tracts that are food deserts within the counties of the catchment area

class food_desert:
    def __init__(self, state = 'Kentucky', region = 'Tract'):
        response = requests.get('https://www.ers.usda.gov/data-products/food-access-research-atlas/download-the-data/')
        soup = BeautifulSoup(response.content, "html.parser")
        hrefs = soup.find_all('a', href = True)
        url_path_series = pd.Series([x['href'] for x in hrefs])
        url = url_path_series[url_path_series.str.match(re.compile('.*FoodAccessResearchAtlasData.*', flags = re.I))].values[0]
        self.path = f'https://www.ers.usda.gov{url}'
        self.region = region
        self.download_data(state = state, region = region)

    def download_data(self, state, region):
        food_desert_link = self.path
        df = pd.read_excel(food_desert_link, sheet_name = 2)
        df = df.loc[df.State.isin(state)].reset_index(drop = True)
        df = df[['CensusTract', 'LILATracts_Vehicle', 'OHU2010']]
        self.original = df.copy()
        if region == 'Tract':
            df.rename(columns = {'CensusTract':'FIPS'}, inplace = True)
            self.food_desert = df
        elif region == 'County':
            self.food_desert['FIPS'] = [str(x)[:5] for x in self.food_desert.CensusTract]
            self.food_desert = df[['FIPS','LILATracts_Vehicle','OHU2010']].groupby('FIPS', as_index = False).apply(lambda x: pd.Series(np.average(x['LILATracts_Vehicle'], weights=x['OHU2010'])))
            self.food_desert.columns = ['FIPS','LILATracts_Vehicle']
            self.food_desert['FIPS'] = self.food_desert.FIPS.astype(int)

    def convert_region(self):
        if self.region == 'Tract':
            df = self.original.copy()
            df['FIPS'] = [str(x).zfill(11)[:5] for x in df.CensusTract]
            df['FIPS'] = df['FIPS']
            self.food_desert = df[['FIPS','LILATracts_Vehicle','OHU2010']].groupby('FIPS', as_index = False).apply(lambda x: pd.Series(np.average(x['LILATracts_Vehicle'], weights=x['OHU2010'])))
            self.food_desert.columns = ['FIPS','LILATracts_Vehicle']
            self.food_desert['FIPS'] = self.food_desert.FIPS.astype(int)
            self.region = 'County'
        elif self.region == 'County':
            df = self.original.copy()
            df.rename(columns = {'CensusTract':'FIPS'}, inplace = True)
            self.food_desert = df
            self.region = 'Tract'

##############################################################################
##############################################################################
##############################################################################
##############################################################################
##############################################################################

# class for cancer incidence and mortality data
# will return all sex, all race for several sites at the county level

class scp_cancer_data:
    import numpy as np

    def __init__(self, state = '21'):
        self.scp_cancer_inc(state=state)
        self.scp_cancer_mor(state=state)

    def scp_cancer_inc(self, state):
        sites = {'001': 'All Site', '071': 'Bladder', '076': 'Brain & ONS', '020': 'Colon & Rectum', '017': 'Esophagus',
                 '072': 'Kidney & Renal Pelvis', '090': 'Leukemia', '035': 'Liver & IBD', '047': 'Lung & Bronchus',
                 '053': 'Melanoma of the Skin', '086': 'Non-Hodgkin Lymphoma', '003': 'Oral Cavity & Pharynx', '040': 'Pancreas',
                 '018': 'Stomach', '080': 'Thyroid'}

        sitesf = {'055': 'Female Breast', '057': 'Cervix', '061': 'Ovary', '058': 'Corpus Uteri & Uterus, NOS'}

        sitesm = {'066': 'Prostate'}

        dat = pd.DataFrame()

        for k, v in sites.items():
            path = f'https://www.statecancerprofiles.cancer.gov/incidencerates/index.php?stateFIPS={state}&areatype=county&cancer={k}&race=00&sex=0&age=001&stage=999&year=0&type=incd&sortVariableName=rate&sortOrder=desc&output=1'
            df = pd.read_csv(path, skiprows=11, header=None, usecols=[0,1,3,9],  names=['County', 'FIPS', 'AAR', 'AAC'],
                             dtype={'County':str, 'FIPS':str}).dropna()
            df['County'] = df['County'].map(lambda x: x.rstrip('\(0123456789\)'))
            df['Site'] = v
            df['Type'] = 'Incidence'
            df = df[['FIPS', 'County', 'Site', 'Type', 'AAR', 'AAC']]
            dat = pd.concat([dat,df], ignore_index=True)

        for k, v in sitesf.items():
            path = f'https://www.statecancerprofiles.cancer.gov/incidencerates/index.php?stateFIPS={state}&areatype=county&cancer={k}&race=00&sex=2&age=001&stage=999&year=0&type=incd&sortVariableName=rate&sortOrder=desc&output=1'
            df = pd.read_csv(path, skiprows=11, header=None, usecols=[0,1,3,9], names=['County', 'FIPS', 'AAR', 'AAC'],
                         dtype={'County':str, 'FIPS':str}).dropna()
            df['County'] = df['County'].map(lambda x: x.rstrip('\(0123456789\)'))
            df['Site'] = v
            df['Type'] = 'Incidence'
            df = df[['FIPS', 'County', 'Site', 'Type', 'AAR', 'AAC']]
            dat = pd.concat([dat,df], ignore_index=True)

        for k, v in sitesm.items():
            path = f'https://www.statecancerprofiles.cancer.gov/incidencerates/index.php?stateFIPS={state}&areatype=county&cancer={k}&race=00&sex=1&age=001&stage=999&year=0&type=incd&sortVariableName=rate&sortOrder=desc&output=1'
            df = pd.read_csv(path, skiprows=11, header=None, usecols=[0,1,3,9], names=['County', 'FIPS', 'AAR', 'AAC'],
                         dtype={'County':str, 'FIPS':str}).dropna()
            df['County'] = df['County'].map(lambda x: x.rstrip('\(0123456789\)'))
            df['Site'] = v
            df['Type'] = 'Incidence'
            df = df[['FIPS', 'County', 'Site', 'Type', 'AAR', 'AAC']]
            dat = pd.concat([dat,df], ignore_index=True)

        dat['FIPS2'] = state
        dat['AAR'] = pd.to_numeric(dat['AAR'], errors='coerce')
        dat['AAC'] = pd.to_numeric(dat['AAC'], errors='coerce')

        self.incidence = dat

    def scp_cancer_mor(self, state):
        sites = {'001': 'All Site', '071': 'Bladder', '076': 'Brain & ONS', '020': 'Colon & Rectum', '017': 'Esophagus',
                 '072': 'Kidney & Renal Pelvis', '090': 'Leukemia', '035': 'Liver & IBD', '047': 'Lung & Bronchus',
                 '053': 'Melanoma of the Skin', '086': 'Non-Hodgkin Lymphoma', '003': 'Oral Cavity & Pharynx', '040': 'Pancreas',
                 '018': 'Stomach', '080': 'Thyroid'}

        sitesf = {'055': 'Female Breast', '057': 'Cervix', '061': 'Ovary', '058': 'Corpus Uteri & Uterus, NOS'}

        sitesm = {'066': 'Prostate'}

        dat = pd.DataFrame()

        for k, v in sites.items():
            path = f'https://www.statecancerprofiles.cancer.gov/deathrates/index.php?stateFIPS={state}&areatype=county&cancer={k}&race=00&sex=0&age=001&year=0&type=death&sortVariableName=rate&sortOrder=desc&output=1'
            df = pd.read_csv(path, skiprows=11, header=None, usecols=[0,1,3,9],  names=['County', 'FIPS', 'AAR', 'AAC'],
                             dtype={'County':str, 'FIPS':str}).dropna()
            df['County'] = df['County'].map(lambda x: x.rstrip('\(0123456789\)'))
            df['Site'] = v
            df['Type'] = 'Mortality'
            df = df[['FIPS', 'County', 'Site', 'Type', 'AAR', 'AAC']]
            dat = pd.concat([dat,df], ignore_index=True)

        for k, v in sitesf.items():
            path = f'https://www.statecancerprofiles.cancer.gov/deathrates/index.php?stateFIPS={state}&areatype=county&cancer={k}&race=00&sex=2&age=001&year=0&type=death&sortVariableName=rate&sortOrder=desc&output=1'
            df = pd.read_csv(path, skiprows=11, header=None, usecols=[0,1,3,9], names=['County', 'FIPS', 'AAR', 'AAC'],
                         dtype={'County':str, 'FIPS':str}).dropna()
            df['County'] = df['County'].map(lambda x: x.rstrip('\(0123456789\)'))
            df['Site'] = v
            df['Type'] = 'Mortality'
            df = df[['FIPS', 'County', 'Site', 'Type', 'AAR', 'AAC']]
            dat = pd.concat([dat,df], ignore_index=True)

        for k, v in sitesm.items():
            path = f'https://www.statecancerprofiles.cancer.gov/deathrates/index.php?stateFIPS={state}&areatype=county&cancer={k}&race=00&sex=1&age=001&year=0&type=death&sortVariableName=rate&sortOrder=desc&output=1'
            df = pd.read_csv(path, skiprows=11, header=None, usecols=[0,1,3,9], names=['County', 'FIPS', 'AAR', 'AAC'],
                         dtype={'County':str, 'FIPS':str}).dropna()
            df['County'] = df['County'].map(lambda x: x.rstrip('\(0123456789\)'))
            df['Site'] = v
            df['Type'] = 'Mortality'
            df = df[['FIPS', 'County', 'Site', 'Type', 'AAR', 'AAC']]
            dat = pd.concat([dat,df], ignore_index=True)

        dat['FIPS2'] = state
        dat['AAR'] = pd.to_numeric(dat['AAR'], errors='coerce')
        dat['AAC'] = pd.to_numeric(dat['AAC'], errors='coerce')

        self.mortality = dat

##############################################################################
##############################################################################
##############################################################################
##############################################################################
##############################################################################

# class for pulling risk factor and screening data from CDC Places
# will return county levela and tract level data

class places_data:


    def __init__(self, state = 'KY'):

        self.places_county(state=state)
        self.places_tract(state=state)

    def places_county(self, state):
        from sodapy import Socrata

        # client = Socrata("chronicdata.cdc.gov",
        #                  "nx4zQ2205wpLwaaaZeZp9zAOs")

        #Example authenticated client
        #(needed for non-public datasets and faster downloads):
        client = Socrata("chronicdata.cdc.gov",
                          "nx4zQ2205wpLwaaaZeZp9zAOs",
                          username="ciodata@uky.edu",
                          password="MarkeyCancer123!")

        # results = client.get("i46a-9kgh", where=f'stateabbr="{state}"')
        results = client.get("kmvs-jkvx", where=f'stateabbr="{state}"')

        results_df = pd.DataFrame.from_records(results)
        results_df2 = results_df.loc[:, results_df.columns.isin(['countyfips', 'countyname', 'stateabbr', 'cancer_crudeprev',
                                  'cervical_crudeprev', 'colon_screen_crudeprev',
                                  'csmoking_crudeprev', 'mammouse_crudeprev', 'obesity_crudeprev'])]

        results_df3 = results_df2.rename(columns={'countyfips': 'FIPS', 'countyname': 'County', 'stateabbr': 'State',
                                                  'cancer_crudeprev': 'Cancer_Prevalence','cervical_crudeprev': 'Met_Cervical_Screen',
                                                  'colon_screen_crudeprev': 'Met_Colon_Screen',
                                                  'mammouse_crudeprev': 'Met_Breast_Screen', 'csmoking_crudeprev': 'Currently_Smoke',
                                                  'obesity_crudeprev': 'BMI_Obese'})

        self.county_est = results_df3

    def places_tract(self, state):
        from sodapy import Socrata

        client = Socrata("chronicdata.cdc.gov",
                          "nx4zQ2205wpLwaaaZeZp9zAOs")

        #Example authenticated client
        #(needed for non-public datasets and faster downloads):
        # client = Socrata("chronicdata.cdc.gov",
        #                  "nx4zQ2205wpLwaaaZeZp9zAOs",
        #                  username="ciodata@uky.edu",
        #                  password="MarkeyCancer123!")

        results = client.get("yjkw-uj5s", where=f'stateabbr="{state}"')
        results_df = pd.DataFrame.from_records(results)
        results_df2 = results_df.loc[:, results_df.columns.isin(['tractfips', 'countyfips', 'countyname',
                                                                 'stateabbr', 'cancer_crudeprev',
                                                                 'colon_screen_crudeprev', 'csmoking_crudeprev',
                                                                 'mammouse_crudeprev', 'obesity_crudeprev'])]

        results_df3 = results_df2.rename(columns={'tractfips': 'FIPS', 'countyfips': 'FIPS5',
                                                  'countyname': 'County',  'stateabbr': 'State',
                                                  'cancer_crudeprev': 'Cancer_Prevalence',
                                                  'colon_screen_crudeprev': 'Met_Colon_Screen',
                                                  'mammouse_crudeprev': 'Met_Breast_Screen',
                                                  'csmoking_crudeprev': 'Currently_Smoke',
                                                  'obesity_crudeprev': 'BMI_Obese'})

        self.tract_est = results_df3
