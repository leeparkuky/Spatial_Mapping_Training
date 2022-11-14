import geopandas as gpd
import pandas as pd
import numpy as np
import pickle
import re
import os
import random
import json
from tqdm import tqdm
from tqdm.auto import trange
import arcgis
from arcgis.gis import GIS, ContentManager
from arcgis.mapping import WebMap
from arcgis.geocoding import batch_geocode, get_geocoders


import warnings
import time

import argparse


class sDataFrame():
    def __init__(self, year = 2019):
        self.year = year
        self._county_gdf = None
        self._tract_gdf = None
        self._tract_gdf_new = None
        
        
    def read_pickle(self, file_path):
        """
        read a pickle file and create a class attribute 'data_dictionary'
        ---
        provide a file_path for a pickle file that contains a dictionary of pandas dataframes
        """
        d = pickle.load(open(file_path,'rb'))
        self.data_dictionary = d
        self.rename_all()
        
    @classmethod
    def read_csv(cls, file_path, names = None):
        cls.data_dictionary = {}
        if isinstance(file_path, list): # if the user provides a list of files (with or without names of each data)
            if names:
                pass
            else:
                names = range(len(file_path))
            for path, name in zip(file_path, names):
                name = str(name)
                df = pd.read_csv(path)
                if df.columns.str.contains(re.compile('.*tract.*', flags = re.I)).sum():
                    if bool(re.match(re.compile(r'.*_tract', flags = re.I), name)):
                        cls.data_dictionary[name] = df
                    else:
                        name = name + '_tract'
                        cls.data_dictionary[name] = df
                else:
                    if bool(re.match(re.compile(r'.*_county', flags = re.I), name)):
                        cls.data_dictionary[name] = df
                    else:
                        name = name + '_county'
                        cls.data_dictionary[name] = df
        elif isinstance(file_path, str):
            df = pd.read_csv(file_path)
            if names:
                pass
            else:
                if df.columns.str.contains(re.compile('.*tract.*', flags = re.I)).sum():
                    names = 'data_tract'
            cls.data_dictionary[names] = df

    @property
    def countyData(self):
        if self.data_dictionary:
            self._countyData = {k:v for k,v in self.data_dictionary.items() if k[-8:] == '(County)'}
            self._countyData['Cancer Incidence - Age Adj.'] = self.data_dictionary['Cancer Incidence - Age Adj.']
            self._countyData['Cancer Mortality - Age Adj.'] = self.data_dictionary['Cancer Mortality - Age Adj.']
            return self._countyData
        else:
            raise AttributeError("data_dictionary has not been created")

            
    @property
    def tractData(self):
        if self.data_dictionary:
            self._tractData  = {k:v for k,v in self.data_dictionary.items() if k[-7:] == '(Tract)' }
            return self._tractData
        else:
            raise AttributeError("data_dictionary has not been created")
            
    @property
    def pointData(self):
        if self.data_dictionary:
            pat = re.compile('.*(cancer|county|tract)', flags = re.I)
            self._pointData = {k:v for k,v in self.data_dictionary.items() if not bool(re.match(pat, k))}
            return self._pointData
        else:
            raise AttributeError("data_dictionary has not been created")

    @property
    def cancerData(self):
        if self.data_dictionary:
            pat = re.compile('.*(cancer).*', flags = re.I)
            self._pointData = {k:v for k,v in self.data_dictionary.items() if (k[-4:] != 'long') & bool(re.match(pat, k))}
            return self._pointData
        else:
            raise AttributeError("data_dictionary has not been created")

            
#################################################################################################################
    def rename_all(self):
        self.rename_econ_columns()
        self.rename_screening_columns()
        self.rename_ht_columns()
        self.rename_sociodemographics_columns()
        self.rename_environment_columns()
        self.rename_cancer_columns()

    def rename_econ_columns(self):
        table_names  = pd.Series(self.data_dictionary.keys())
        table_names  = table_names[table_names.str.contains('econ', flags = re.I)].values
        for table in table_names:
            econ = self.data_dictionary[table].copy()
            economic_columns = {'Insurance Coverage': 'Insured',
                               'Medicaid Enrollment': 'Enrolled in Medicaid',
                               'Gini Coefficient': 'Gini Coefficient',
                               'Household Income': 'Household Income ($)',
                               'Annual Labor Force Participation Rate (2015-2019)': 'Annual Labor Forace Participation (2015-2019)',
                               'Annual Unemployment Rate (2015-2019)': 'Annual Unemployment (2015-2019)',
                                'Below Poverty': 'Living Below Poverty',
                               'Uninsured': 'Uninsured',}
            keys = econ.columns[econ.dtypes.ne('object')]
            cond = keys.str.contains('monthly unemployment rate', flags = re.I)
            values = [economic_columns[key] if not con else key.replace('Rate ', '') for key, con in zip(keys, cond) ]
            econ.rename(columns = dict(zip(keys, values)), inplace = True)
            self.data_dictionary[table] = econ
                    
    def rename_screening_columns(self):
        table_names  = pd.Series(self.data_dictionary.keys())
        table_names  = table_names[table_names.str.contains('risk factors', flags = re.I)].values
        for table in table_names:
            screening = self.data_dictionary[table].copy()
            screening_columns = {'BMI_Obese': 'Obese (BMI over 30)',
                               'Cancer_Prevalence': 'Cancer Prevalence',
                               'Currently_Smoke': 'Currently Smoke (Adult)',
                               'Met_Breast_Screen': 'Met Breast Screening Recommendations',
                               'Met_Cervical_Screen': 'Had Pap Test in Last 3 Years, Age 21-64',
                               'Met_Colon_Screen': 'Met Colorectal Screening Recommendations'}
            screening.rename(columns = screening_columns, inplace = True)
            self.data_dictionary[table] = screening
                    
    def rename_ht_columns(self):
        table_names  = pd.Series(self.data_dictionary.keys())
        table_names  = table_names[table_names.str.contains('housing', flags = re.I)].values
        for table in table_names:
            ht = self.data_dictionary[table].copy()
            ht_columns = {'Vacancy Rate': 'Vacancy Rate',
                               'No Vehicle': 'Household without Vehicle Access',
                               'Rent Burden (40% Income)': 'High Rent Burden',}
            ht.rename(columns = ht_columns, inplace = True)
            self.data_dictionary[table] = ht
                
                
    def rename_sociodemographics_columns(self):
        table_names  = pd.Series(self.data_dictionary.keys())
        table_names  = table_names[table_names.str.contains('sociodemographics', flags = re.I)].values
        for table in table_names:
            sdemo = self.data_dictionary[table].copy()
            sdemo_columns = {'Total': 'Total Population',
                             'Under 18': 'Under 18 Years Old',
                             '18 to 64': 'Age 18 to 64 Years Old',
                             'Over 64': 'Over 64 Years Old',
                            'Below 9th grade':'Did Not Attend High School',
                            'High School': 'Graduated High School',
                            'College':'Graduated College',
                            'Advanced Degree':'Completed Graduate Degree',
                            'White': 'White (Non-Hispanic)',
                            'Black': 'Black (Non-Hispanic)',
                            'Asian': 'Asian (Non-Hispanic)',
                            'Other_Races':'Other Non-Hispanic Races',
                            'Urban_Percentage': 'Urbanized Residence'}
            sdemo.rename(columns = sdemo_columns, inplace = True)
            self.data_dictionary[table] = sdemo
                
                
    def rename_environment_columns(self):
        table_names  = pd.Series(self.data_dictionary.keys())
        table_names  = table_names[table_names.str.contains('environment', flags = re.I)].values
        for table in table_names:
            env = self.data_dictionary[table].copy()
            env_columns = {'PWS_Violations_Since_2016': 'Public Water System Violations Since 2016',
                          'LILATracts_Vehicle': 'Tracts that are Food Deserts',
                          'LILA_Tracts_Vehicle': 'Tracts that are Food Deserts'}
            env.rename(columns = env_columns, inplace = True)
            self.data_dictionary[table] = env
                
                
                
    def rename_cancer_columns(self):
        table_names  = pd.Series(self.data_dictionary.keys())
        table_names  = table_names[table_names.str.contains('cancer', flags = re.I)].values
        for table in table_names:
            cancer = self.data_dictionary[table].copy()
            cancer_columns = {'All Site': 'All Cancer Site',
                         'Bladder': 'Bladder',
                         'Brain & ONS': 'Brain',
                         'Cervix': 'Cervical',
                         'Colon & Rectum': 'Colorectal',
                         'Corpus Uteri & Uterus, NOS': 'Uterine',
                         'Esophagus': 'Esophageal',
                         'Female Breast': 'Female Breast',
                         'Kidney & Renal Pelvis': 'Kidney',
                         'Leukemia': 'Leukemia',
                         'Liver & IBD': 'Liver',
                         'Lung & Bronchus': 'Lung',
                         'Melanoma of the Skin': 'Melanoma',
                         'Non-Hodgkin Lymphoma': 'Non-Hodgkin Lymphoma',
                         'Oral Cavity & Pharynx': 'Head & Neck Cancer',
                         'Ovary': 'Ovarian',
                         'Pancreas': 'Pancreatic',
                         'Prostate': 'Prostate',
                         'Stomach': 'Stomach',
                         'Thyroid': 'Thyroid'}
            cancer.rename(columns = cancer_columns, inplace = True)
            self.data_dictionary[table] = cancer
                
                
###################################################################################################################                
            
    @staticmethod
    def find_state_fips(examples, idx = 0):
        examples = {k: v for k,v in examples.items() if not k[-5:] =='_long' }
        state_fips = examples[list(examples.keys())[idx]].reset_index().FIPS.astype(str).apply(lambda x: x[:2]).unique().tolist()
        return state_fips
    
    @staticmethod
    def find_county_fips(examples, idx = 0):
        county_fips = examples[list(examples.keys())[idx]].reset_index().FIPS.unique().tolist()
        return county_fips

    
    @property
    def state_fips(self):
        if self.data_dictionary:
            self._state_fips = self.find_state_fips(self.data_dictionary)
            return self._state_fips
        else:
            raise AttributeError("data_dictionary has not been created")
            
    @property
    def county_fips(self):
        if self.data_dictionary:
            self._county_fips = self.find_county_fips(self.countyData)
            return self._county_fips
        else:
            raise AttributeError("data_dictionary has not been created")


    @property
    def tiger_census_county(self):
        county = f'https://www2.census.gov/geo/tiger/TIGER{self.year}/COUNTY/tl_{self.year}_us_county.zip'
        gdf = gpd.read_file(county)
        gdf = gdf.loc[gdf.STATEFP.isin(self.state_fips), ['GEOID','geometry']]
        gdf.GEOID = gdf.GEOID.astype(int)
        self._county_gdf =  gdf.reset_index(drop = True)
        return self._county_gdf
        
    @property
    def tiger_census_tract(self):
        states = self.state_fips
        county_fips = self.county_fips
        final_gdf = self.get_tiger_census(self.year, states, county_fips)
        self._tract_gdf = final_gdf
        self._tract_num = final_gdf.GEOID.unique().shape[0]
        return self._tract_gdf
    
    @property
    def next_tiger_census_tract(self):
        states = self.state_fips
        county_fips = self.county_fips
        year = self.year + 1
        final_gdf = self.get_tiger_census(year, states, county_fips)
        self._tract_gdf_new = final_gdf
        self._tract_num_new = final_gdf.GEOID.unique().shape[0]
        return self._tract_gdf_new

    
    @staticmethod
    def get_tiger_census(year, state, county_FIPS = None):
        def transform_gdf(gdf, county_fips):
            county_fips = [str(x) for x in county_fips]
            if county_fips is not None:
                gdf = gdf.loc[countyfp.isin(county_fips), ['GEOID','geometry']]
            else:
                gdf = gdf.loc[:,  ['GEOID','geometry']]
            return gdf
        
        if isinstance(state, int) or isinstance(state, str):
            tract = f"https://www2.census.gov/geo/tiger/TIGER{year}/TRACT/tl_{year}_{state}_tract.zip"
            gdf = gpd.read_file(tract)
            countyfp = gdf.STATEFP + gdf.COUNTYFP
            gdf = transform_gdf(gdf, county_FIPS)
            gdf.GEOID = gdf.GEOID.astype(int)
            final_gdf = gdf.reset_index(drop = True)
            return final_gdf
        elif isinstance(state, list):
            if len(state) == 1:
                state = state[0]
                tract = f"https://www2.census.gov/geo/tiger/TIGER{year}/TRACT/tl_{year}_{state}_tract.zip"
                gdf = gpd.read_file(tract)
                countyfp = gdf.STATEFP + gdf.COUNTYFP
                gdf = transform_gdf(gdf, county_FIPS)
                gdf.GEOID = gdf.GEOID.astype(int)
                final_gdf = gdf.reset_index(drop = True)
                return final_gdf
            elif len(state) > 1:
                dataframes = []
                for s in state:
                    tract = f"https://www2.census.gov/geo/tiger/TIGER{year}/TRACT/tl_{year}_{s}_tract.zip"
                    gdf = gpd.read_file(tract)
                    countyfp = gdf.STATEFP + gdf.COUNTYFP
                    gdf = transform_gdf(gdf, county_FIPS)
                    gdf.GEOID = gdf.GEOID.astype(int)
                    dataframes.append(gdf.reset_index(drop = True))
                final_gdf = pd.concat(dataframes)
                return final_gdf
            else:
                raise TypeError("No element in state")

                
                
#######################################################################################################################                
                
                             
class CIFTool_AGOL(sDataFrame):
    def __init__(self, gis_address, user_name, password, folder_name = None):
        self.gis_address = gis_address
        self.user_name = user_name
        self.password = password
        self._contentManager = None
        super().__init__()
        self.layers = {}
        self.layers_id = {}
        self.countyLayers = {}
        self.tractLayers = {}
        self.pointLayers = {}
        self.tractLayers_id = {}
        self.countyLayers_id = {}
        self.webmaps = {}
        self.groupLayers = {}
        self.groupLayersCP = {}
        self.error_count = 0
        self.wait = True
        self.set_folder(folder_name = folder_name)
        self.rng = random.Random()

    def wait_AGOL(self, waitTime = None, verbose = True, desc = None):
        if waitTime == None:
            waitTime = self.rng.random()*self.rng.randint(1,3)
        if self.wait:
            if verbose:
                for p in trange(30, desc = desc, leave = False):
                    time.sleep(round(waitTime/30,5))
            else:
                time.sleep(waitTime)
        else:
            pass
        
        
    @staticmethod
    def sociodemographic_colname_update(df):
        df = df.rename(columns = {'Total':'Total Population',
                            '18 to 64' : 'Age 18 to 64'})
        return df
    
    
    
    @classmethod
    def from_layers(cls, layers, gis_address, client_id):
        obj = cls(gis_address, client_id)
        contentManager = obj.gis
        obj.layers = {}
        keys = list(layers.keys())
        values = list(layers.values())
        for t1 in trange(len(keys), desc = f"Of {len(keys)} groups"):
            key = keys[t1]
            obj.layers[keys[t1]] = {}
            if len(values[t1]) > 0:
                subkey = list(values[t1].keys())
                subvalues = list(values[t1].values())
                for t2 in trange(len(layers[key]), desc = f"Of {len(layers[key])} tables"):
                    k = subkey[t2]
                    layer_id = subvalues[t2]
#         for key, item in layers.items():
#             obj.layers[key] = {}
#             if len(item) > 0:
#                 for t in trange(len(layers)):
#                     k = list(layers.keys())[t]
#                     layer_id = list(layers.items())[t]
#                 for k, layer_id in layers[key].items():
                    group_lyr =  contentManager.search(layer_id, "Feature Layer")
                    obj.wait_AGOL(1, verbose = False)
                    if len(group_lyr) == 0:
                        raise ValueError("No such group_lyr")
                    else:
                        g_lyr = group_lyr[0]
                        lyr = g_lyr.layers[0]
                        obj.layers[key][k] = lyr
        return obj
        
    @classmethod
    def from_pickle(cls, file_path, gis_address, client_id):
        layers = pickle.load(open(file_path,'rb'))
        obj = cls(gis_address, client_id)
        contentManager = obj.gis
        obj.layers = {}
        keys = list(layers.keys())
        values = list(layers.values())
        for t1 in trange(len(keys), desc = f"Of {len(keys)} groups"):
            key = keys[t1]
            obj.layers[keys[t1]] = {}
            if len(values[t1]) > 0:
                subkey = list(values[t1].keys())
                subvalues = list(values[t1].values())
                for t2 in trange(len(layers[key]), desc = f"Of {len(layers[key])} tables"):
                    k = subkey[t2]
                    layer_id = subvalues[t2]
#         for key, item in layers.items():
#             obj.layers[key] = {}
#             if len(item) > 0:
#                 for k, layer_id in layers[key].items():
                    group_lyr =  contentManager.search(layer_id, "Feature Layer")
                    obj.wait_AGOL(1, verbose = False)
                    if len(group_lyr) == 0:
                        raise ValueError("No such group_lyr")
                    else:
                        flag = True
                        while flag:
                            try:
                                g_lyr = group_lyr[0]
                                lyr = g_lyr.layers[0]
                                obj.layers[key][k] = lyr
                                flag = False
                            except:
                                obj.wait_AGOL(3)
                                obj.error_count += 1
                                if obj.error_count >= 5:
                                    contentManager = obj.gis
                                    obj.error_count = 0
        return obj
            
        
    
    def genTractMaps(layers):
        pass
    
    def genCountyMaps(self):
        pass
    
    
    
    
    @staticmethod
    def manage_popups(wm, level = 'county', title  = None):
        for lyr in wm.layers:
            if title is None:
                lyr.popupInfo.title = lyr.popupInfo.title.replace("_", " ").title()
            elif isinstance(title, str):
                lyr.popupInfo.title = title
            else:
                raise ValueError("title must be a string")

            main_fields = [f.label for f in lyr.popupInfo.fieldInfos if f.fieldName != f.label]
            try:
                main_fields.remove("Fips")
            except:
                pass
            for field in lyr.popupInfo.fieldInfos:
                if field.label.islower():
                    field.label = field.label.title()
                if level.lower() == 'county':
                    labels_to_include  = ['County','State'] + main_fields
                elif level.lower() == 'tract':
                    labels_to_include = ['Tract','County','State'] + main_fields
                else:
                    raise ValueError('level must be either "county" or "tract"')

                if field.label in labels_to_include:
                    field.visible = True
                else:
                    field.visible = False
        return wm    
    
    
    def genWebMapFromGroupLayers(self, level= 'county'):
        if self._contentManager:
            contentManager = self._contentManager
        else:
            contentManager = self.gis
        area_layers = {key:value for key, value in self.groupLayersCP.items() if not bool(re.match("Facilities.+", key))}
        if level.lower() == 'county':
            area_layers = {key:value for key, value in area_layers.items() if not bool(re.match(".+(Tract)", key))}
        elif level.lower() == 'tract':
            area_layers = {key:value for key, value in area_layers.items() if bool(re.match(".+(Tract)", key))}
        else:
            raise ValueError("level can ben either 'county' or 'tract'")
        fl_names = list(area_layers.keys())
        fl_dicts = list(area_layers.values())
        fl_length = len(fl_names)
        for j in trange(fl_length):
            flname = fl_names[j]
            fl_dict = fl_dicts[j]
            webmap_ids = {}
            GL_id = fl_dict['id']
            GL = contentManager.search(GL_id)[0]
#         for flname, fl_dict in area_layers.items():
            # first edit alias of each field
            if j == 0:
                extent = dict(zip(['xmin','ymin','xmax','ymax'], GL.extent[0]+ GL.extent[1]))
                extent['spatialReference'] = GL.spatialReference
            lyr = GL.layers[0]
            properties = lyr.properties
            original_colname = fl_dict['alias']
            AGOLNames = fl_dict['fields']
            sdf_colname = [x.name for x in properties.fields]
            colname_dict = dict(zip(original_colname, AGOLNames))

            for colname in fl_dict['alias']:
                AGOLName = colname_dict[colname]
                idx = sdf_colname.index(AGOLName)
#                 idx, AGOLName = self.getAGOLFieldName(lyr, colname)
                properties['fields'][idx]['alias'] = colname
            lyr.properties = properties
            time.sleep(.25)
            # next, define a renderer for each and create webmaps
            for colname, title in zip(fl_dict['fields'], fl_dict['alias']):
                renderer = self.renderer_definition(lyr, colname)
                new_renderer = self.add_class_details(renderer)
                flag = True
                while flag:
                    try:
                        wm = WebMap()
                        wm.add_layer(lyr, {'title': f'{flname} : {title}',
                                           'renderer': new_renderer})
                        # wm.add_layer(item, {'title': f'{title} : {key}',
                        #                    'renderer' : new_renderer})
                        self.wait_AGOL(.5, desc = f'Adding the layer "{flname}:{title}" to the map')
                        wm = self.manage_popups(wm, level = level, title = f"{flname} : {title}")
                        self.wait_AGOL(.5, desc = f"Managing Popup")
                        wm = self.addPointLayers(wm, start_index = 1)
                        webmap_item_properties = {'title': f'{flname} : {title}',
                                                 'snippet':f'{flname} : {title}',
                                                 'tags' : ['Python','Automated'], 
                                                  'extent': extent
                                                 }
                        webmap = wm.save(webmap_item_properties, folder = self.AGOL_folder) # testing folder is temporary set-up
                        self.wait_AGOL(.5, desc = f'Creating a WebMap "{flname}:{title}"')
#                         searchedWebMap = contentManager.search(f'title:"{flname} : {title}"', item_type = 'Web Map')
#                         webmap_s = searchedWebMap[0]
#                         webmap_s.share(everyone = True)
#                         self.wait_AGOL(.5, desc = f"Chaning sharing option")
                        webmap_id = webmap.id
                        webmap_ids[title] = webmap_id
                        flag = False
                        self.wait_AGOL(1, desc = f"Complete: {flname}-{title}")
                    except:
                        time.sleep(2)
                        self.error_count += 1
                        if self.error_count >= 5:
                            contentManager = self.gis
                            self.error_count = 0
            self.webmaps[flname] = webmap_ids
            del self.groupLayersCP[flname]
        self.shareWebMaps(level = level)
        return self.webmaps
    
    def shareWebMaps(self, level = 'all'):
        if self._contentManager:
            contentManager = self._contentManager
        else:
            contentManager = self.gis
            
        area_layers = [key for key in self.groupLayers.keys() if not bool(re.match("Facilities.+", key))]
        if level.lower() == 'county':
            area_layers = [key for key in area_layers if not bool(re.match(".+(Tract)", key))]
        elif level.lower() == 'tract':
            area_layers = [key for key in area_layers if bool(re.match(".+(Tract)", key))]
        elif level.lower() == 'all':
            pass
        else:
            raise ValueError("level can ben either 'county','tract', or 'all'")            
            
            
        map_names = []
        map_ids = []
        for group_name, maps in self.webmaps.items():
            if group_name in area_layers:
                for map_name, map_id in maps.items():
                    map_names.append(f"{group_name}:{map_name}")
                    map_ids.append(map_id)
                
        for i in trange(len(map_ids), leave = False, desc = "sharing webmaps"):
            map_id = map_ids[i]
            wm = self._contentManager.search(map_id)[0]
            time.sleep(.25)
            wm.share(everyone = True)
            
        
        
    def genWebMap(self, layers, title):
        if self._contentManager:
            contentManager = self._contentManager
        else:
            contentManager = self.gis
        wm = WebMap()
        for key, item in layers.items():
            # step 1. set-up alias for each layer
            idx, AGOLName = self.getAGOLFieldName(item, key)
            item.properties['fields'][idx]['alias'] = key
            time.sleep(.25) # give a time to update the property
            # ste 2. set-up renderer with 5 classes
            renderer = self.renderer_definition(item, AGOLName)
            new_renderer = self.add_class_details(renderer)
            # flag = True
            # while flag:
            #     try:
            #         item.manager.update_definition(new_renderer)
            #         self.wait_AGOL(3, desc = f"Updaing the definition of {title}-{key}')
            #         flag = False
            #     except:
            #         time.sleep(2)
            #         self.error_count += 1
            #         if self.error_count >= 5:
            #             contentManager = self.gis
            #             self.error_count = 0
            # item.properties['drawingInfo']['renderer'] = new_renderer
            flag = True
            while flag:
                try:
                    wm.add_layer(item, {'title': f'{title} : {key}',
                                       'renderer': new_renderer})
                    # wm.add_layer(item, {'title': f'{title} : {key}',
                    #                    'renderer' : new_renderer})
                    self.wait_AGOL(5, desc = f'Adding the layer "{title}:{key}" to the map')
                    flag = False
                except:
                    time.sleep(2)
                    self.error_count += 1
                    if self.error_count >= 5:
                        contentManager = self.gis
                        self.error_count = 0
        return wm

        
    @staticmethod
    def getFieldNames(lyr):
        field_names = [x['name'] for x in lyr.properties.fields]
        return field_names
    
    
    @staticmethod
    def getAGOLFieldName(lyr, FieldName):
        flag = True
        while flag:
            try:
                field_names = [x['name'] for x in lyr.properties.fields]
                flag = False
            except:
                time.sleep(2)
        FieldName = re.sub('\s[()&]\s*','__',FieldName)
        FieldName = re.sub('[()]','', FieldName)
        FieldName = re.sub('-','_', FieldName)
        AGOLName = FieldName.lower().replace(' ', '_')[:10]
        try:
            index = field_names.index(AGOLName)
        except:
            sdf = lyr.query().sdf
            cond = sdf.columns.str.contains('^' + AGOLName[:3])
            AGOLName = sdf.columns[cond].values[0]
            index = field_names.index(AGOLName)
        return index, AGOLName
#         lyr.properties['fields'][index]['alias'] = FieldName

    
    @staticmethod
    def renderer_definition(lyr, fieldName):
        num_unique = len(lyr.query().sdf[fieldName].unique()) 
        if num_unique >= 5:
            definition = {
                'type':'classBreaksDef',
                'classificationField':fieldName,
                'classificationMethod':'esriClassifyNaturalBreaks',
                'breakCount':5,
                    }
        else:
            definition = {
                'type':'classBreaksDef',
                'classificationField':fieldName,
                'classificationMethod':'esriClassifyNaturalBreaks',
                'breakCount':num_unique,
                    }        
        flag = True
        while flag:
            try:
                renderer = lyr.generate_renderer(definition)
                flag = False
            except:
                time.sleep(5)
        return renderer
        
    @staticmethod
    def add_class_details(renderer):
        num = len(renderer['classBreakInfos'])
        def edit_labels(renderer):
            labels = [x['label'].split(' - ') for x in renderer['classBreakInfos']]
            min_label = labels[0][0]
            try:
                max_label = labels[-1][1]
            except:
                max_label = labels[-1][0]
            if float(min_label) >= 0 and float(max_label) <= 1 and len(labels) > 2:
                new_labels = []
                for label in labels:
                    l = round(float(label[0])*100,2)
                    u = round(float(label[0])*100,2)
                    l = str(l); u = str(u)
                    l += '%'; u += '%'
                    new_labels.append(' - '.join([l, u]))
                return new_labels
            else:
                return None

        
        class1 = [237, 248, 233, 216]
        class2 = [186, 228, 179, 216]
        class3 = [116, 196, 118, 216]
        class4 = [49, 163, 84, 216]
        class5 = [0, 109, 44, 216]

        color_scheme = [class1, class2, class3, class4, class5]

        default_symbol = {
                "type" : "esriSFS", 
                "style" : "esriSFSSolid", 
                "color" : [], 
                'outline': {'type': 'esriSLS', 
                            'style': 'esriSLSSolid', 
                            'color': [52, 52, 52, 255],
                            "width" : 0.4}
                    }

        new_labels = edit_labels(renderer)
        if num == 3:
            color_scheme.remove(class2)
            color_scheme.remove(class4)
        elif num == 2:
            color_scheme.remove(class2)
            color_scheme.remove(class3)
            color_scheme.remove(class4)
        else:
            pass
        for i, class_info in enumerate(renderer['classBreakInfos']):
            if new_labels:
                class_info['label'] = new_labels[i]
            symbol = default_symbol.copy()
            symbol['color'] = color_scheme[i]
            class_info['symbol'] = symbol


        return renderer    
    
#     def create_webmap(self, title, )
    
        
    def genFeatureLayer(self, sdf, title):
        if self._contentManager:
            contentManager = self._contentManager
        else:
            contentManager = self.gis
        lyrs = contentManager.import_data(sdf,
                                  title = title,
                                 folder = self.AGOL_folder)
        self.wait_AGOL(desc = f"Waiting to Upload {title} layer to AGOL")
        time.sleep(.5)
        return lyrs
    
    
    @property
    def gis(self):
        warnings.simplefilter('ignore')
        gis = GIS(self.gis_address, self.user_name, self.password) # log-in to the AGOL
        print("Successfully logged in as: " + gis.properties.user.username)
        self._gis = gis
        self._contentManager = ContentManager(gis)
        return self._contentManager
                
    
    def set_folder(self, folder_name):
        try:
            gis = self._gis
        except:
            self.gis
            gis = self._gis
        if folder_name is None:
            import random
            import string
            # printing lowercase
            letters = string.ascii_lowercase
            # Webmaps - UTC timestamp
            # folder_name = ''.join(random.choice(letters) for i in range(10))
            folder_name = time.ctime()
            gis.content.create_folder(folder=folder_name)
            print(f"folder name is {folder_name}")
            self.AGOL_folder = folder_name

        elif isinstance(folder_name, str):
            x = gis.content.create_folder(folder=folder_name)
            if isintance(x, dict):
                pass
            else:
                print("{folder_name} already exists")
            self.AGOL_folder = folder_name
        else:
            raise Error("folder name has to be in the string format")
            

    
    
    
    def genCountyFL(self, verbose = True):
        data_dictionary = self.countyData
        if verbose:
            keys = list(data_dictionary.keys())
            for t1 in trange(len(keys), desc = f"Generating {len(keys)} county-level feature layers groups"):
                key = keys[t1]
                df = data_dictionary[key]
                self.genAreaSDF4FL(df, key, 'county')
        else:
            keys = list(data_dictionary.keys())
            for key in keys:
                df = data_dictionary[key]
                self.genAreaSDF4FL(df, key, 'county')
            
    def genTractFL(self, verbose = True):
        data_dictionary = self.tractData
        print(data_dictionary.keys())
        if verbose:
            keys = list(data_dictionary.keys())
            for t1 in trange(len(keys), desc = f"Generating {len(keys)} tract-level feature layers groups"):
                key = keys[t1]
                df = data_dictionary[key]
                self.genAreaSDF4FL(df, key, 'tract')
        else:
            keys = list(data_dictionary.keys())
            for key in keys:
                df = data_dictionary[key]
                self.genAreaSDF4FL(df, key, 'tract')
        
    
    def genPointFL(self, agg = True):
        final_sdf = self.batchgeocode(df = self.pointData['Facilities and Providers'])
        if agg:
            cond_gi_providers = final_sdf.Type.str.contains('(Colon & Rectal Surgeon)|(Obstetrics & Gynecology)|(Gastroenterology)')
            cond_FQHD         = final_sdf.Type.str.contains("(FQHC)|(HPSA)")
            final_sdf.loc[cond_gi_providers, "Type"] = "GI Providers"
            final_sdf.loc[cond_FQHD, "Type"]         = "FQHCs / Other HPSA"
        
        facility_types = list(final_sdf.Type.unique())
        
        for k in trange(len(facility_types)):
            facility = facility_types[k]
#         for facility in final_sdf['Type'].unique():
            sdf = final_sdf.loc[final_sdf.Type.eq(facility),:]
            group_layer = self.genFeatureLayer(sdf, title = f"Facilities and Providers : {facility}")
            self.wait_AGOL(2, desc = f"Uploading Point Feature Layer : {facility}")
            pointLayer = group_layer.layers[0]
            self.updateLayerName(pointLayer, facility)
            item_id = pointLayer.properties.serviceItemId

            self.pointLayers[facility] = {'Layer Object': pointLayer,
            'id': item_id}
            self.groupLayers[f"Facilities and Providers : {facility}"] = {'Layer Object': pointLayer,
            'id': item_id}
            #############################################################################################
            # 1. Can we adjust colors? Nope
            # 2. Need to change the FL name -> complete
            # 3. Need to change popup -> Yes..
            #############################################################################################
    
    def batchgeocode(self,df, address_column_name = "Address"):
#         geocoded_address = geocode(address)
        try:
            self.SuggestedBatchSize
        except:
            self.set_geocoder()
        
        df = df.reset_index(drop = True)
        df['State'] = df[address_column_name].str.extract("\s(\w\w)\s\d\d\d\d\d")
        states = df.State.value_counts().head(len(self.state_fips)).index.tolist()
        df = df.loc[df.State.isin(states),:]

        address = df[address_column_name]
                
        SBS = self.SuggestedBatchSize
        N = len(address)
        
        if N/SBS == N//SBS:
            numSplits = N/SBS
        else:
            numSplits = N//SBS + 1
        
        SHAPES = []
        for i in range(numSplits):
            start = i*SBS
            end = start + SBS
            sample = address[start:end].tolist()
            feature_set = batch_geocode(sample, as_featureset = True)
            feature_set = feature_set.sdf
            feature_set['ResultID'] = feature_set.ResultID + start
            feature_set.set_index('ResultID', inplace = True)
            shape = feature_set.SHAPE
            SHAPES.append(shape)
        
        shapes = pd.concat(SHAPES)
        df['SHAPE'] = None
        df.loc[df.index.isin(shapes.index), 'SHAPE'] = shapes.sort_index()
        df = df.dropna().reset_index(drop = True)
        df.drop('State', axis = 1, inplace = True)
        return df
    
    def set_geocoder(self):
        try:
            gis = self._gis
        except:
            self.gis
            gis = self._gis
        geocoder = get_geocoders(gis)[0]
        self.SuggestedBatchSize = geocoder.properties.locatorProperties.SuggestedBatchSize

    
    def genAreaSDF4FL_new(self, df, name, level = 'county', tqdm = None, keep_NA = True):
        """
        with a county or tract level dataset, it creates feature layer for each column.
        Then, it uploads each feature layer to AGOL. 
        The name of the Feature Layer will be {name}-{column name in a df}
        """
        if len(df.index.names)>1:
            df.reset_index(inplace = True)
        
        if level == 'county':
            if self._county_gdf is not None:
                gdf = self._county_gdf.copy()
            else:
                gdf = self.tiger_census_county.copy()
        elif level == 'tract':
            if self._tract_gdf is not None:
                gdf = self._tract_gdf.copy()
            else:
                gdf = self.tiger_census_tract.copy()
            if df.FIPS.unique().shape[0] != self._tract_num:
                if self._tract_gdf_new is not None:
                    gdf = self._tract_gdf_new.copy()
                else:
                    gdf = self.next_tiger_census_tract.copy()
                    #####################################################################################
                if df.FIPS.unique().shape[0] != self._tract_num_new: # This needs to be updated later
                    diff1 = -df.FIPS.unique().shape[0] + self._tract_num
                    diff2 = -df.FIPS.unique().shape[0] + self._tract_num_new
                    if diff1>=0:
                        gdf = self._tract_gdf.copy()
                    else:
                        gdf = self._tract_gdf_new.copy()
#                     raise ValueError("Tracts for {self.year} and {self.year +1} are not matching your dataset")
        ###########################3
        else:
            raise ValueError("level has to be either 'county' or 'tract'")
            
            
            
        df['FIPS'] = df.FIPS.astype(int)
#         if df.columns.str.contains('Total').sum() > 0:
#             df = self.sociodemographic_colname_update(df) # to be changed
#         if df.columns.str.contains(re.compile('LILAT.*')).sum() > 0:
#             df = df.rename(columns = {'LILATracts_Vehicle' : 'LILA Tracts Vehicle'}) ## to be changed  
        sdf = df.merge(gdf, how = 'left', left_on = 'FIPS', right_on = 'GEOID') # we need to pick up from here
        sdf.drop('GEOID', axis = 1, inplace = True) # drop the geoid
        sdf = gpd.GeoDataFrame(sdf, geometry = 'geometry')
        sdf = pd.DataFrame.spatial.from_geodataframe(sdf, column_name = 'geometry')
        self.wait_AGOL(.5, verbose = False)
        geo_pat = re.compile(r'(fips|tract|county|state|geometry|type)', flags = re.I)
        food_desert_pat = re.compile(r'tract.+food\sdesert', flags = re.I)
        # geo_pat2 = re.compile(r'(fips|tract|county|state|type)', flags = re.I)
        columns     = sdf.columns[(~sdf.columns.str.match(geo_pat)) + sdf.columns.str.match(food_desert_pat)].to_list()
        final_sdf = sdf.copy() 
        if keep_NA:
            cond = final_sdf.isna().sum().eq(final_sdf.shape[0])
            columns_to_drop = final_sdf.columns[cond].tolist()
            if len(columns_to_drop) > 0:
                for x in columns_to_drop:
                    columns.remove(x)
                final_sdf.drop(columns_to_drop, axis = 1, inplace = True)
                self.data_dictionary[name] = df.drop(columns_to_drop, axis = 1)
                if level == 'county':
                    self.countyData[name] =  df.drop(columns_to_drop, axis = 1)
                else:
                    self.tractData[name] =  df.drop(columns_to_drop, axis = 1)
        else:
            nulls = -final_sdf.isna().apply(lambda x: True if sum(x) > 0 else False, axis = 1)
            final_sdf = final_sdf.loc[nulls, :]
        self.final_sdf = final_sdf
        ls = None
        while ls is None:
            groupLayer = self.genFeatureLayer(final_sdf, name)
            ls = groupLayer.layers
            if ls is None:
                self.error_count += 1
            if self.error_count >= 5:
                contentManager = self.gis
                self.error_count = 0
        simpleLayer = groupLayer.layers[0]
        self.updateLayerName(simpleLayer, name)
        item_id = simpleLayer.properties.serviceItemId
        fields_not_considering = ['FID','fips','county','state','tract','type','Shape__Area','Shape__Length'] 
        fields = [x['name'] for x in simpleLayer.properties.fields if x['name'] not in fields_not_considering]
        self.groupLayers[name] = {'id' : item_id,
                                  'fields': fields,
                                 'alias': columns}
        self.layers[name] = item_id
        self.groupLayersCP[name] = {'id' : item_id,
                                  'fields': fields,
                                 'alias': columns}
    
    
    

    def genAreaSDF4FL(self, df, name, level = 'county', tqdm = None, keep_NA = False):
        """
        with a county or tract level dataset, it creates feature layer for each column.
        Then, it uploads each feature layer to AGOL. 
        The name of the Feature Layer will be {name}-{column name in a df}
        """
        if len(df.index.names)>1:
            df.reset_index(inplace = True)
        
        if level == 'county':
            if self._county_gdf is not None:
                gdf = self._county_gdf.copy()
            else:
                gdf = self.tiger_census_county.copy()
        elif level == 'tract':
            if self._tract_gdf is not None:
                gdf = self._tract_gdf.copy()
            else:
                gdf = self.tiger_census_tract.copy()
            if df.FIPS.unique().shape[0] != self._tract_num:
                if self._tract_gdf_new is not None:
                    gdf = self._tract_gdf_new.copy()
                else:
                    gdf = self.next_tiger_census_tract.copy()
                if df.FIPS.unique().shape[0] != self._tract_num_new: # This needs to be updated later
                    diff1 = -df.FIPS.unique().shape[0] + self._tract_num
                    diff2 = -df.FIPS.unique().shape[0] + self._tract_num_new
                    if diff1>=0:
                        gdf = self._tract_gdf.copy()
                    else:
                        gdf = self._tract_gdf_new.copy()
#                     raise ValueError("Tracts for {self.year} and {self.year +1} are not matching your dataset")
        else:
            raise ValueError("level has to be either 'county' or 'tract'")
        self.layers[name] = {}
        layers = {}
        layer_ids = {}
        df['FIPS'] = df.FIPS.astype(str)
        gdf['GEOID'] = gdf.GEOID.astype(str)
#         if df.columns.str.contains('Total').sum() > 0:
#             df = self.sociodemographic_colname_update(df)
        sdf = df.merge(gdf, how = 'left', left_on = 'FIPS', right_on = 'GEOID') # we need to pick up from here
        sdf.drop('GEOID', axis = 1, inplace = True) # drop the geoid
        sdf = gpd.GeoDataFrame(sdf, geometry = 'geometry')
        sdf = pd.DataFrame.spatial.from_geodataframe(sdf, column_name = 'geometry')
        self.wait_AGOL(.5, verbose = False)
        geo_pat = re.compile(r'(fips|tract|county|state|geometry|type)', flags = re.I)
        geo_pat2 = re.compile(r'(fips|tract|county|state|type)', flags = re.I)
        columns     = sdf.columns[~sdf.columns.str.match(geo_pat)].to_list()
        geo_columns = sdf.columns[sdf.columns.str.match(geo_pat2)].to_list()
        for colname in columns:
            if tqdm is not None:
                tqdm.set_description(f"{name} to AGOL - {colname} in progress")
            selected_columns = geo_columns + [colname, 'geometry']
            final_sdf = sdf[selected_columns]
            if keep_NA:
                final_sdf.loc[final_sdf[colname].isna(), colname] = None
            else:
                final_sdf = final_sdf.loc[final_sdf[colname].notna(), :]
            self.final_sdf = final_sdf

            if name:
                layer_name = name + '-' + colname
            else:
                layer_name = colname
            ls = None
            while ls is None:
                groupLayer = self.genFeatureLayer(final_sdf, layer_name )
                ls = groupLayer.layers
                if ls is None:
                    self.error_count += 1
                if self.error_count >= 5:
                    contentManager = self.gis
                    self.error_count = 0
                
            simpleLayer = groupLayer.layers[0]
            self.updateLayerName(simpleLayer, layer_name)
            item_id = simpleLayer.properties.serviceItemId
            self.layers_id[name][colname] = item_id
            self.layers[name][colname] = simpleLayer
            layers[colname] = simpleLayer
            layer_ids[colname] = item_id
        if level == 'county':
            self.countyLayers[name] = layers
            self.countyLayers_id[name] = layer_ids
        elif level == 'tract':
            self.tractLayers[name] = layers
            self.tractLayers_id[name] = layer_ids
            
        return layers
    
    
    def updateLayerName(self, lyr, name):
        flag = True
        while flag:
            try:
                lyr.manager.update_definition({'name':name})
                self.wait_AGOL(3, desc = f"Updating layer name to '{name}'")
                flag = False
            except:
                self.wait_AGOL(3, desc = "Trying one more time")
                self.error_count += 1
                if self.error_count >= 5:
                    contentManager = self.gis
                    self.error_count = 0
                    
                    
    def addPointLayers(self, webmap, start_index = 0):
        if self._contentManager:
            contentManager = self._contentManager
        else:
            contentManager = self.gis
        facilities = [x for x in self.groupLayers.keys() if bool(re.match("Facilities.+", x))]
        ids = [self.groupLayers[x]['id'] for x in facilities]
        colors = {'Facilities and Providers : FQHCs / Other HPSA':[49,191,10,255],
                 'Facilities and Providers : Lung Cancer Screening':[255,255,255,255],
                 'Facilities and Providers : GI Providers': [191,10,48,255],
                 'Facilities and Providers : Mammography':[100,59,65,255]}
        for facility, id_num in zip(facilities, ids):
            PL = contentManager.search(id_num, item_type = "Feature Layer Collection")[0]
            pl = PL.layers[0]
            color = colors[facility]
            renderer = self.point_renderer(color)
            pl.manager.properties.drawingInfo.renderer = renderer
            pl.properties.drawingInfo.renderer         = renderer
            webmap.add_layer(pl)
        for i in range(start_index, start_index + 3):
            webmap.layers[i].popupInfo.title = '{name} ({type})'
            webmap.layers[i].popupInfo.description = "<b>Address</b> : {address} <br> <b>Phone Number</b> : {phone_numb}"
        
        return webmap                    
                    
                    
                    
    def shareFL(self):
        if self._contentManager:
            contentManager = self._contentManager
        else:
            contentManager = self.gis
        id_numbers = [x['id'] for x in list(self.groupLayers.values())]
        for id_num in id_numbers:
            item  = contentManager.search(id_num, item_type = 'Feature Layer Collection', max_items = 100)
            item  = item[0]
            item.share(everyone = True)
    
    @staticmethod
    def point_renderer(color, outline_color = None, size = 6):
        if outline_color is None:
            outline_color = [0,0,0,255]
        else:
            if isinstance(outline_color, list):
                if len(outline_color) != 4:
                    raise Error("outline_color is a list of 4 integers indicating RGB colors and opacity")
                else:
                    pass
            else:
                raise Error("outline_color is a list of 4 integers indicating RGB colors and opacity")

        renderer = {
                  "type": "simple",
                  "symbol": {
                    "type": "esriSMS",
                    "style": "esriSMSCircle",
                    "color": color,
                    "size": size,
                    "angle": 0,
                    "xoffset": 0,
                    "yoffset": 0,
                    "outline": {
                      "color": outline_color,
                      "width": 0.7
                    }
                  }
        }
        # renderer.symbol.url = ''
        # renderer.symbol.imageData = ''
        renderer = arcgis._impl.common._mixins.PropertyMap(renderer)
        return renderer

                    
                    
                    
                    

    def save_layers(self, file_name, directory = None):
        if len(self.layers) == 0:
            raise ValueError("There is no layer in this object")
        else:
            if directory == None:
                path1 = os.getcwd()
            if file_name[-7:] != '.json':
                file_name += '.json'
            with open(os.path.join(path1,file_name), 'w') as f:
                json.dump(self.layers, f)
                
                
    def save_webmaps(self, file_name, directory = None):
        if len(self.webmaps) == 0:
            raise ValueError("There is no maps in this object")
        else:
            if directory == None:
                path1 = os.getcwd()
            if file_name[-5:] != '.json':
                file_name += '.json'
            with open(file_name, 'w') as f:
                json.dump(self.webmaps, f)

    def __call__(self, pickle_file_path):
        self.read_pickle(pickle_file_path)
        print("Generating Web Feature Layers for Facilities")
        self.genPointFL()
        
        names = list(self.countyData.keys())
        dfs    = list(self.countyData.values())
        print("Generating Web Feature Layers for County Level Data")
        for i in trange(len(names)):
            df = dfs[i]
            name = names[i]
            self.genAreaSDF4FL_new(df, name, 'county', keep_NA = True)
        
        print("Generating Web Feature Layers for Tract Level Data")
        names = list(self.tractData.keys())
        dfs    = list(self.tractData.values())
        for i in trange(len(names)):
            df = dfs[i]
            name = names[i]
            self.genAreaSDF4FL_new(df, name, 'tract', keep_NA = True)
        
        self.shareFL()
        print("Generating Web Maps with County Level Feature Layers")

        try:
            self.genWebMapFromGroupLayers(level = 'county')
            
        except:
            self.gis
            self.genWebMapFromGroupLayers(level = 'county')
            
        print("Generating Web Maps with Tract Level Feature Layers")
        try:
            self.genWebMapFromGroupLayers(level = 'tract')
            
        except:
            self.gis
            self.genWebMapFromGroupLayers(level = 'tract')     
            
        self.save_webmaps(file_name = 'WebMaps')
        self.save_layers(file_name = 'GroupLayers')
            



######################################################################################################################




if __name__ == "__main__" :
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('-g','--gis_address', help='arcgis online address for the organization (e.g. https://ky-cancer.maps.arcgis.com')
    parser.add_argument('-i', '--user_name', help = 'AGOL/portal user name')
    parser.add_argument('-p', '--password', help = 'AGOL/portal password')
    parser.add_argument('-f','--pickle_input_file_path', help = 'pickle file to read')
    parser.add_argument('--csv_file_paths', nargs = '+')
    parser.add_argument('--csv_file_names', nargs = '+')

    args = parser.parse_args()

    # if not args.gis_address:
    #     gis_address = "https://ky-cancer.maps.arcgis.com"
    # if not args.client_id:
    #     app_id = 'xXa3NxkwXPlLIrX5'
    #
    # gis = GIS(gis_address, client_id = app_id) # log-in to the AGOL
    # print("Successfully logged in as: " + gis.properties.user.username)
    if args.csv_file_paths:
        s = sDataFrame()
        s.read_csv(args.csv_file_paths, names = args.csv_file_names)
        print(s.data_dictionary.keys())


    if args.pickle_input_file_path:
        cif = CIFTool_AGOL( args.gis_address, args.user_name, args.password)
        cif(args.pickle_input_file_path)
