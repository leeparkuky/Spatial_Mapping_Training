# -*- coding: utf-8 -*-
"""
Program to receive inputs and utilize CIFTools.py module
for collection and curation of cancer rates and other related data.
"""

__authors__ = ["Todd Burus, MAS", "Lee Park, MS"]
__copyright__ = "Copyright 2022, University of Kentucky"


import pandas as pd
import os
import sys
from io import StringIO
# CIFTools updated to z_annotated_ciftools below for testing
from z_annotated_ciftools import census_sdoh as sdoh
from z_annotated_ciftools import BLS
from z_annotated_ciftools import food_desert
from z_annotated_ciftools import fcc
from z_annotated_ciftools import facilities
from z_annotated_ciftools import water_violation
from z_annotated_ciftools import scp_cancer_data
from z_annotated_ciftools import places_data
import argparse
import glob
import pickle

#################################################
### customize paths and files before running  ###
#################################################

if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    # hyperparameters sent by the client are passed as command-line arguments to the script.
    parser.add_argument("-f", "--input_folder", type=str, default = os.getcwd())
    parser.add_argument("-c", "--ca_name", type = str, default = None)
    parser.add_argument("-o", "--output_folder", type=str, default= None)
    parser.add_argument("-t", "--ca_file_type", type=str, default='csv')
    parser.add_argument("-i", "--interactive", type = str, default = 'yes')

    """
    Example of parser:
    python CIF_pull_data.py --input_folder ./input_folder --ca_name Dartmouth_xlsx_test_nov_22 --ca_file_type 'xlsx' -i no
    OR
    python z_annotated_cifpull.py --input_folder ./input_folder --ca_name Dartmouth_xlsx_test_nov_22 --ca_file_type xlsx -i no
    """
    ## INTERPRETATION OF CODE ABOVE
    # then args.input_folder = "./input_folder"
    # and args.ca_name = "Dartmouth"
    # default value kept: args.output_folder = None
    # args.ca_file_type = 'csv' (by default)
    # NOT interactive

    args, _ = parser.parse_known_args()
    if args.interactive.lower() == 'yes':
        ### input default download path for Chrome
        dl_path = input(r'What is your default download folder for Google Chrome?: ') # C:\\Users\\usr\\Downloads\\
        ### input file name for catchment area county list
        ca_file = input(r'What file contains your catchment area counties?: ') # uky_ca.csv
        ### input name of catchment area
        ca_name = input(r'Give a short name to identify your cancer center in save files: ') # Markey
        ca_dir = ca_name.replace(" ", "_") + "_catchment_data"
        path2 = os.path.join(os.getcwd(), ca_dir)
        if os.path.exists(path2) == False:
            os.makedirs(path2)

    else:
        input_path = args.input_folder
        # find a csv file in the input_folder
        print(os.path.join(input_path, f'*.{args.ca_file_type}')) # zella note: for testing only
        ca_file = glob.glob(os.path.join(input_path, f'*.{args.ca_file_type}'))[0]
        chromedriver_name = 'chromedriver'
        if sys.platform.lower()[:3] == 'win':
        	chromedriver_name += '.exe'
        drive_file = glob.glob(os.path.join(input_path, chromedriver_name), recursive = True)[0]
        print(f'google chrome driver and the catchment area counties file should be in {input_path}')
        print(f"file path of catchment are file is : {ca_file}")
        print(f"chrome drive file is locaed in : {drive_file}")
        if not args.ca_name:
            ca_name = 'cancer_center'
        else:
            ca_name = args.ca_name
        ca_dir = ca_name.replace(" ", "_") + "_catchment_data"
        if not args.output_folder:
            path2 = os.path.join(os.getcwd(), ca_dir)
        else:
            path2 = args.output_folder
        if os.path.exists(path2) == False:
            os.makedirs(path2)
        dl_path = os.path.join(path2, 'src')
        if os.path.exists(dl_path) == False:
            os.makedirs(dl_path)
        print(f"final datasets will be in {path2}")
        print(f"data files downaloded through web scraping will be saved in : {dl_path}")


    #################################################

    ### create table and dataframe of states
    state = '''State,FIPS2,StateAbbrev
    Alabama,01,AL
    Alaska,02,AK
    Arizona,04,AZ
    Arkansas,05,AR
    California,06,CA
    Colorado,08,CO
    Connecticut,09,CT
    Delaware,10,DE
    District of Columbia,11,DC
    Florida,12,FL
    Georgia,13,GA
    Hawaii,15,HI
    Idaho,16,ID
    Illinois,17,IL
    Indiana,18,IN
    Iowa,19,IA
    Kansas,20,KS
    Kentucky,21,KY
    Louisiana,22,LA
    Maine,23,ME
    Maryland,24,MD
    Massachusetts,25,MA
    Michigan,26,MI
    Minnesota,27,MN
    Mississippi,28,MS
    Missouri,29,MO
    Montana,30,MT
    Nebraska,31,NE
    Nevada,32,NV
    New Hampshire,33,NH
    New Jersey,34,NJ
    New Mexico,35,NM
    New York,36,NY
    North Carolina,37,NC
    North Dakota,38,ND
    Ohio,39,OH
    Oklahoma,40,OK
    Oregon,41,OR
    Pennsylvania,42,PA
    Rhode Island,44,RI
    South Carolina,45,SC
    South Dakota,46,SD
    Tennessee,47,TN
    Texas,48,TX
    Utah,49,UT
    Vermont,50,VT
    Virginia,51,VA
    Washington,53,WA
    West Virginia,54,WV
    Wisconsin,55,WI
    Wyoming,56,WY
    '''

    dfCsv = StringIO(state)

    stateDf = pd.read_csv(dfCsv, sep=',', dtype={'State':str, 'FIPS2':str, 'StateAbbrev':str})
    stateDf.State = stateDf.State.str.strip()

    ### subset for catchment area
    ca = pd.read_csv(ca_file, dtype={'FIPS':str})
    ca = pd.merge(ca, stateDf, on='State', how='left')
    caState = ca.State.unique().tolist()
    caSA = ca.StateAbbrev.unique().tolist()
    caFIPS = ca.FIPS.unique().tolist()
    caStateFIPS = ca.FIPS2.unique().tolist()

    ### run county sdoh function for catchment area or all US counties
    sdoh_county_df = dict()

    try:
        for s in caStateFIPS:
            print(f'Collecting county-level Census data for {s}')
            sdoh_county = sdoh(region = 'County', state=s, run_query = True, year = 2020)
            if len(sdoh_county_df) == 0:
                sdoh_county_df = sdoh_county.sdoh_df
            else:
                for k, v in sdoh_county.sdoh_df.items():
                    sdoh_county_df[k] = pd.concat([sdoh_county_df[k],
                                                   sdoh_county.sdoh_df[k]])
            del sdoh_county
            for k, v in sdoh_county_df.items():
                sdoh_county_df[k] = sdoh_county_df[k][sdoh_county_df[k]['FIPS'].isin(caFIPS)]
    except NameError:
        for s in stateDf.FIPS2:
            sdoh_county = sdoh(region = 'County', state=s, run_query = True, year = 2020)
            if len(sdoh_county_df) == 0:
                sdoh_county_df = sdoh_county.sdoh_df
            else:
                for k, v in sdoh_county.sdoh_df.items():
                    sdoh_county_df[k] = pd.concat([sdoh_county_df[k], sdoh_county.sdoh_df[k]])
            del sdoh_county


    ### run tract sdoh function for catchment area or all US Census tracts
    sdoh_tract_df = dict()

    try:
        for s in caStateFIPS:
            print(f'Collecting Census tract-level Census data for {s}')
            sdoh_tract = sdoh(region = 'Tract', state=s, run_query = True, year = 2020)
            if len(sdoh_tract_df) == 0:
                sdoh_tract_df = sdoh_tract.sdoh_df
            else:
                for k, v in sdoh_tract.sdoh_df.items():
                    sdoh_tract_df[k] = pd.concat([sdoh_tract_df[k], sdoh_tract.sdoh_df[k]])
            del sdoh_tract
            for k, v in sdoh_tract_df.items():
                sdoh_tract_df[k]['FIPS5'] = sdoh_tract_df[k]['FIPS'].str[0:5]
                sdoh_tract_df[k] = sdoh_tract_df[k][sdoh_tract_df[k]['FIPS5'].isin(caFIPS)]
                sdoh_tract_df[k] = sdoh_tract_df[k].drop(columns=['FIPS5'])
    except NameError:
        for s in stateDf.FIPS2:
            sdoh_tract = sdoh(region = 'Tract', state=s, run_query = True, year = 2020)
            if len(sdoh_tract_df) == 0:
                sdoh_tract_df = sdoh_tract.sdoh_df
            else:
                for k, v in sdoh_tract.sdoh_df.items():
                    sdoh_tract_df[k] = pd.concat([sdoh_tract_df[k], sdoh_tract.sdoh_df[k]])
            del sdoh_tract


    ### run county monthly unemployment for catchment area of all US counties
    bls_df = pd.DataFrame()

    try:
        for s in caStateFIPS:
            print(f'Collecting county-level labor statistics for {s}')
            bls = BLS(state = s)
            bls.df[f'Monthly Unemployment Rate ({bls.df.Period.unique()[0]})'] = bls.df['Unemployment Rate']*0.01
            bls.df = bls.df.drop(columns=['Unemployment Rate', 'Period'])
            bls.df = bls.df[bls.df['FIPS'].isin(caFIPS)]
            bls_df = pd.concat([bls_df, bls.df], ignore_index=True)
            del bls
    except NameError:
        for s in stateDf.FIPS2:
            bls = BLS(state = s)
            bls.df[f'Monthly Unemployment Rate ({bls.df.Period.unique()[0]})'] = bls.df['Unemployment Rate']*0.01
            bls.df = bls.df.drop(columns=['Unemployment Rate', 'Period'])
            bls_df = pd.concat([bls_df, bls.df], ignore_index=True)
            del bls


    #################################################
    # Define functions for curating data ############
    #################################################

    ### economic data
    def gen_econ_data(countyDf = sdoh_county_df, tractDf = sdoh_tract_df):
        print('Generating economic data tables...')

        econ_county = countyDf['demo_all'].loc[:, :'State'].sort_values('FIPS').reset_index(drop = True)
        econ_tract = tractDf['demo_all'].loc[:, :'State'].sort_values('FIPS').reset_index(drop = True)

        # add insurnace
        econ_county = econ_county.merge(countyDf['insurance'], on=['FIPS', 'County', 'State'], how='left')
        econ_tract = econ_tract.merge(tractDf['insurance'], on=['FIPS', 'Tract', 'County', 'State'], how='left')

        # add gini_index
        econ_county = econ_county.merge(countyDf['gini_index'], on=['FIPS', 'County', 'State'], how='left')
        econ_tract = econ_tract.merge(tractDf['gini_index'], on=['FIPS', 'Tract', 'County', 'State'], how='left')

        # add median_household_income
        econ_county = econ_county.merge(countyDf['income'].loc[:,:'median_income_all'],\
                                        on=['FIPS', 'County', 'State'], how='left')
        econ_tract = econ_tract.merge(tractDf['income'].loc[:,:'median_income_all'],\
                                      on=['FIPS', 'Tract', 'County', 'State'], how='left')

        # add annual_unemployment
        econ_county = econ_county.merge(countyDf['employment'], on=['FIPS', 'County', 'State'], how='left')
        econ_tract = econ_tract.merge(tractDf['employment'], on=['FIPS', 'Tract', 'County', 'State'], how='left')

        # add poverty
        econ_county = econ_county.merge(countyDf['poverty'], on=['FIPS', 'County', 'State'], how='left')
        econ_county = econ_county.drop(columns=['below_poverty_x.5', 'below_poverty_x2'])

        econ_tract = econ_tract.merge(tractDf['poverty'], on=['FIPS', 'Tract', 'County', 'State'], how='left')
        econ_tract = econ_tract.drop(columns=['below_poverty_x.5', 'below_poverty_x2'])

        # rename columns
        colnames = {'Labor Force Participation Rate': 'Annual Labor Force Participation Rate (2015-2019)',
                    'Unemployment Rate' : 'Annual Unemployment Rate (2015-2019)',
                    'health_insurance_coverage_rate': 'Insurance Coverage',
                    'Gini Index': 'Gini Coefficient',
                    'median_income_all': 'Household Income',
                    'medicaid' : 'Medicaid Enrollment',
                    'below_poverty' : 'Below Poverty'
                    }

        econ_county.rename(columns = colnames, inplace = True)
        econ_tract.rename(columns = colnames, inplace = True)

        # calculate uninsured
        econ_county['Uninsured'] = 1-econ_county['Insurance Coverage']
        econ_tract['Uninsured'] = 1-econ_tract['Insurance Coverage']

        # monthly unemployment
        econ_county = econ_county.merge(bls_df, on='FIPS', how='left')

        return({'county': econ_county, 'tract':econ_tract})


    ### housing and transportation data
    def gen_housing_transportation_data(countyDf = sdoh_county_df, tractDf = sdoh_tract_df):
        print('Generating housing and transportation data tables...')

        # vacancy
        housing_county = countyDf['vacancy'].sort_values('FIPS').reset_index(drop = True)
        housing_tract = tractDf['vacancy'].sort_values('FIPS').reset_index(drop = True)

        # transporation
        housing_county = \
            housing_county.merge(countyDf['transportation'].loc[:,:'no_vehicle'], \
                                 on=['FIPS', 'County', 'State'], how='left')
        housing_tract = housing_tract.merge(tractDf['transportation'].loc[:,:'no_vehicle'], \
                                            on=['FIPS', 'Tract', 'County', 'State'], how='left')

        # rent_to_income
        housing_county = housing_county.merge(countyDf['rent_to_income'], \
                                              on=['FIPS', 'County', 'State'], how='left')
        housing_tract = housing_tract.merge(tractDf['rent_to_income'], \
                                            on=['FIPS', 'Tract', 'County', 'State'], how='left')

        housing_county.rename(columns = {'vacancy_rate': 'Vacancy Rate', 'no_vehicle': 'No Vehicle',
                                         'rent_over_40':'Rent Burden (40% Income)'}, inplace = True)
        housing_tract.rename(columns = {'vacancy_rate': 'Vacancy Rate', 'no_vehicle': 'No Vehicle',
                                     'rent_over_40':'Rent Burden (40% Income)'}, inplace = True)
        housing_county.sort_values('FIPS', inplace = True)
        housing_tract.sort_values('FIPS', inplace = True)
        return({'ht_county': housing_county, 'ht_tract': housing_tract})



    ### sociodemographic data
    # create function to add race/ethnicity
    def add_race(table, sdoh_df, race):
        table = table.sort_values('FIPS')
        dat = sdoh_df[f'demo_{race}'].sort_values('FIPS')[['FIPS', 'total']].rename(columns={'total': f'{race}'})
        table = table.merge(dat, on='FIPS', how='left')
        return(table)

    # gather sociodemographic data
    def gen_sociodemographic_data(countyDf = sdoh_county_df, tractDf = sdoh_tract_df):
        print('Generating sociodemographic data tables...')

        # population
        sociodemo_county = countyDf['demo_total'].sort_values('FIPS').reset_index(drop = True)
        sociodemo_tract = tractDf['demo_total'].sort_values('FIPS').reset_index(drop = True)

        #education
        sociodemo_county = sociodemo_county.merge(countyDf['education'], \
                                                  on=['FIPS', 'County', 'State'], how='left')
        sociodemo_tract = sociodemo_tract.merge(tractDf['education'], \
                                                on=['FIPS', 'Tract', 'County', 'State'], how='left')

        # race/ethnicity
        race = ['White','Black','Hispanic','Asian','Other_Races']
        for r in race:
            sociodemo_county = add_race(sociodemo_county, countyDf, r)
            sociodemo_tract = add_race(sociodemo_tract, tractDf, r)

        # % rural
        sociodemo_county = sociodemo_county.merge(countyDf['urban_rural'], \
                                                  on=['FIPS', 'County', 'State'], how='left')
        sociodemo_county.sort_values('FIPS', inplace = True)
        sociodemo_tract.sort_values('FIPS', inplace = True)
        return({'county': sociodemo_county, 'tract': sociodemo_tract})



    ### gather location data
    def gen_location_data():
        print('Collecting provider and facility location data...')
        if not ca.empty:
            place = caSA
            place2 = caFIPS
        else:
            place = stateDf.StateAbbrev
            place2 = stateDf.StateAbbrev

        point_df = facilities()

        print('Collecting HPSA facility data...')
        point_df.hpsa(location = place2)

        print('Collecting FQHC data...')
        point_df.fqhc(location = place2)

        print('Collecting provider data...')
        point_df.nppes(location = place)

        print('Collecting mammography facility data...')
        point_df.mammography(state = place)

        print('Collecting lung cancer screening facility data...')
        if drive_file:
            point_df.lung_cancer_screening(download_path = dl_path, chrome_driver_path = drive_file, location = place)
        else:
            point_df.lung_cancer_screening(download_path = dl_path, location = place)

        try:
            lcs = point_df.lung_cancer_screening_df
        except:
            lcs = point_df.return_lung_cancer_screening_data()
        nppes = point_df.nppes_df
        mammo = point_df.mammography_df
        hpsa = point_df.hpsa_df
        fqhc = point_df.fqhc_df
        point_df = pd.concat([lcs, nppes, mammo, hpsa, fqhc]).sort_values('Type')

        return(point_df)



    ### gather environmental data
    def gen_env_data(water_violation_start_year = 2016):
        print('Collecting environmental data...')
        env_county = pd.DataFrame()
        # water violations
        print('Collecting safe drinking water violations...')
        for s in caSA:
            water = water_violation(state = s, start_year = 2016)
            env_county = pd.concat([env_county, water.df])
        # env_county['FIPS'] = env_county.FIPS.astype(int)
        env_county.rename(columns ={'counts': f'PWS_Violations_Since_{water_violation_start_year}' }, inplace = True)
        env_county = env_county.merge(stateDf, on='StateAbbrev', how='left')
        env_county = sdoh_county_df['poverty'].merge(env_county, on=['County', 'State'], how='left')
        env_county = env_county[['FIPS', 'County', 'State', f'PWS_Violations_Since_{water_violation_start_year}']]

        # broadband speeds
        print('Collecting broadband data...')
        fcc_data = pd.DataFrame()
        print(dl_path) # ZELLA NOTE: for testing only
        for s in caSA:
            try:
            # if drive_file: # ZELLA NOTE: throwing an error: NameError: name 'drive_file' is not defined
                FCC = fcc(state=s, download_path = dl_path, chrome_driver_path = drive_file)
            except NameError:
            # else:
                FCC = fcc(state=s, download_path = dl_path)
            fcc_data = pd.concat([fcc_data, FCC.fcc_data], ignore_index=True)
            del FCC

        fcc_data['FIPS'] = fcc_data['BlockCode'].astype(str).str[:5]
        fcc_data = fcc_data[fcc_data['FIPS'].isin(caFIPS)]
        fcc_data = fcc_data.drop(columns='FIPS')

        # food_desert
        print('Collecting food desert data...')
        food  = food_desert(state = caState)
        env_tract = food.food_desert
        env_tract['Census_Tract_2019'] = env_tract.FIPS.astype(str).str.zfill(11)
        env_tract['FIPS'] = env_tract['Census_Tract_2019'].str[:5]
        env_tract = env_tract[['FIPS', 'Census_Tract_2019','LILATracts_Vehicle']]
        env_tract = sdoh_county_df['poverty'].iloc[:,:3].merge(env_tract, on = 'FIPS', how='left')
        env_tract.sort_values('FIPS', inplace = True)

        print('Aggregating food desert data to county-level...')
        food.convert_region()
        county_food = food.food_desert
        county_food['FIPS'] = county_food.FIPS.astype(str).str.zfill(5)
        env_county = env_county.iloc[:,:4].merge(county_food, on='FIPS', how='left')
        env_county.sort_values('FIPS', inplace = True)

        return({'environment_county': env_county, 'environment_tract': env_tract,
                'broadband_speeds': fcc_data})

    ### gather cancer data
    def gen_cancer_data():
        print('Collecting cancer incidence and mortality data...')
        inc_data = pd.DataFrame()
        mor_data = pd.DataFrame()

        for s in caStateFIPS:
            cnr = scp_cancer_data(state = s)
            inc_data = pd.concat([inc_data, cnr.incidence], ignore_index=True)
            mor_data = pd.concat([mor_data, cnr.mortality], ignore_index=True)
            del cnr

        inc_data = inc_data.merge(stateDf, on='FIPS2', how='left')
        caInc = inc_data[['FIPS', 'County', 'State', 'Type', 'Site', 'AAR', 'AAC']]
        caInc = caInc[caInc['FIPS'].isin(caFIPS)]

        mor_data = mor_data.merge(stateDf, on='FIPS2', how='left')
        caMor = mor_data[['FIPS', 'County', 'State', 'Type', 'Site', 'AAR', 'AAC']]
        caMor = caMor[caMor['FIPS'].isin(caFIPS)]

        return({'cancer_incidence': caInc, 'cancer_mortality': caMor})

    ### gather CDC Places data
    def gen_places_data():
        print('Collecting risk factor and screening data...')
        places_county_data = pd.DataFrame()
        places_tract_data = pd.DataFrame()

        for s in caSA:
            places = places_data(state = s)
            places_county_data = pd.concat([places_county_data, places.county_est], ignore_index=True)
            places_tract_data = pd.concat([places_tract_data, places.tract_est], ignore_index=True)
            del places

        placesCounty = places_county_data[places_county_data['FIPS'].isin(caFIPS)]
        placesCounty_l = pd.melt(placesCounty, id_vars=['FIPS', 'County', 'State'],
                                 var_name='measure', value_name='value')
        placesCounty_l['value'] = pd.to_numeric(placesCounty_l['value'])/100

        placesTract = places_tract_data[places_tract_data['FIPS5'].isin(caFIPS)]
        placesTract = placesTract.drop(columns=['FIPS5'])
        placesTract_l = pd.melt(placesTract, id_vars=['FIPS', 'County', 'State'],
                                var_name = 'measure', value_name = 'value')
        placesTract_l['value'] = pd.to_numeric(placesTract_l['value'])/100

        return({'rfs_county': placesCounty_l, 'rfs_tract': placesTract_l})

    #################################################
    # Compile Data and Write to File ################
    #################################################

    ### compile data
    def comp_data():
        rfs = gen_places_data()
        rfs_county_l, rfs_tract_l = rfs['rfs_county'], rfs['rfs_tract']
        rfs_county = pd.pivot(rfs_county_l, index=['FIPS', 'County', 'State'], columns='measure', values='value')
        rfs_tract = pd.pivot(rfs_tract_l, index=['FIPS', 'County', 'State'], columns='measure', values='value')
        cancer = gen_cancer_data()
        cancer_inc_l, cancer_mor_l = cancer['cancer_incidence'], cancer['cancer_mortality']
        cancer_inc = pd.pivot(cancer_inc_l, index=['FIPS', 'County', 'State', 'Type'], columns='Site', values='AAR')
        cancer_mor = pd.pivot(cancer_mor_l, index=['FIPS', 'County', 'State', 'Type'], columns='Site', values='AAR')
        econ = gen_econ_data()
        econ_county, econ_tract = econ['county'], econ['tract']
        econ_county_l = pd.melt(econ_county, id_vars = ['FIPS', 'County', 'State'],
                                var_name = 'measure', value_name = 'value')
        econ_tract_l = pd.melt(econ_tract, id_vars = ['FIPS', 'Tract', 'County', 'State'],
                                var_name = 'measure', value_name = 'value')
        housing_transportation = gen_housing_transportation_data()
        ht_county, ht_tract = housing_transportation['ht_county'], housing_transportation['ht_tract']
        ht_county_l = pd.melt(ht_county, id_vars = ['FIPS', 'County', 'State'],
                                var_name = 'measure', value_name = 'value')
        ht_tract_l = pd.melt(ht_tract, id_vars = ['FIPS', 'Tract', 'County', 'State'],
                                var_name = 'measure', value_name = 'value')
        sociodemo = gen_sociodemographic_data()
        sociodemo_county, sociodemo_tract = sociodemo['county'], sociodemo['tract']
        sd_county_l = pd.melt(sociodemo_county, id_vars = ['FIPS', 'County', 'State'],
                                var_name = 'measure', value_name = 'value')
        sd_tract_l = pd.melt(sociodemo_tract, id_vars = ['FIPS', 'Tract', 'County', 'State'],
                                var_name = 'measure', value_name = 'value')
        env = gen_env_data()
        env_county, env_tract, broadband_data = env['environment_county'], env['environment_tract'], env['broadband_speeds']
        env_county_l = pd.melt(env_county, id_vars = ['FIPS', 'County', 'State'],
                                var_name = 'measure', value_name = 'value')
        env_tract_l = pd.melt(env_tract, id_vars = ['FIPS', 'County', 'State', 'Census_Tract_2019'],
                                var_name = 'measure', value_name = 'value')
        point_df = gen_location_data()

        return({'rf_and_screening_county': rfs_county, 'rf_and_screening_county_long': rfs_county_l,
                'rf_and_screening_tract': rfs_tract, 'rf_and_screening_tract_long': rfs_tract_l,
                'cancer_incidence': cancer_inc, 'cancer_incidence_long': cancer_inc_l,
                'cancer_mortality': cancer_mor, 'cancer_mortality_long': cancer_mor_l,
                'economy_county': econ_county, 'economy_county_long': econ_county_l,
                'economy_tract': econ_tract, 'economy_tract_long': econ_tract_l,
                'ht_county': ht_county, 'ht_county_long': ht_county_l,
                'ht_tract': ht_tract, 'ht_tract_long': ht_tract_l,
                'sociodemographics_county': sociodemo_county, 'sd_county_long': sd_county_l,
                'sociodemographics_tract': sociodemo_tract, 'sd_tract_long': sd_tract_l,
                'environment_county': env_county, 'environment_county_long': env_county_l,
                'environment_tract': env_tract, 'environment_tract_long': env_tract_l,
                'broadband_speeds': broadband_data, 'facilities_and_providers': point_df})


    def comp_data4AGOL():
        rfs = gen_places_data()
        rfs_county_l, rfs_tract_l = rfs['rfs_county'], rfs['rfs_tract']
        rfs_county = pd.pivot(rfs_county_l, index=['FIPS', 'County', 'State'], columns='measure', values='value').reset_index()
        rfs_tract = pd.pivot(rfs_tract_l, index=['FIPS', 'County', 'State'], columns='measure', values='value').reset_index()
        cancer = gen_cancer_data()
        cancer_inc_l, cancer_mor_l = cancer['cancer_incidence'], cancer['cancer_mortality']
        cancer_inc = pd.pivot(cancer_inc_l, index=['FIPS', 'County', 'State', 'Type'], columns='Site', values='AAR').reset_index()
        cancer_mor = pd.pivot(cancer_mor_l, index=['FIPS', 'County', 'State', 'Type'], columns='Site', values='AAR').reset_index()
        econ = gen_econ_data()
        econ_county, econ_tract = econ['county'], econ['tract']
        housing_transportation = gen_housing_transportation_data()
        ht_county, ht_tract = housing_transportation['ht_county'], housing_transportation['ht_tract']
        sociodemo = gen_sociodemographic_data()
        sociodemo_county, sociodemo_tract = sociodemo['county'], sociodemo['tract']
        env = gen_env_data()
        env_county, env_tract, broadband_data = env['environment_county'], env['environment_tract'], env['broadband_speeds']
        env_tract.drop('FIPS', axis = 1, inplace = True)
        env_tract.rename(columns = {'Census_Tract_2019': 'FIPS',
                                    'LILATracts_Vehicle': 'LILA_Tracts_Vehicle'}, inplace = True)
        env_tract = env_tract[['FIPS','County','State','LILA_Tracts_Vehicle']]
        point_df = gen_location_data()

        cdata = {'Screening & Risk Factors (County)': rfs_county,
                'Screening & Risk Factors (Tract)': rfs_tract,
                'Cancer Incidence - Age Adj.': cancer_inc,
                'Cancer Mortality - Age Adj.': cancer_mor,
                'Economic Indicators (County)': econ_county,
                'Economic INdicators (Tract)': econ_tract,
                'Housing & Transportation (County)': ht_county,
                'Housing & Transportation (Tract)': ht_tract,
                'Sociodemographics (County)': sociodemo_county,
                'Sociodemographics (Tract)': sociodemo_tract,
                'Environment (County)': env_county,
                'Environment (Tract)': env_tract,
                'Broadband Speed': broadband_data,
                'Facilities and Providers': point_df}

        with open(os.path.join(path2,'dataset.pickle'), 'wb') as dataset:
            pickle.dump(cdata, dataset, protocol=pickle.HIGHEST_PROTOCOL)

            # ZELLA NOTE: seems like around here that we should edit to create csv and excel files as well

    ### write data to Excel
    def save_as_xlsx(cdata:dict) -> None:
        from pandas import ExcelWriter
        from datetime import datetime as dt


        save_name = ca_name.replace(" ", "_") + '_catchment_data_' + dt.today().strftime('%m-%d-%Y') + '.xlsx'
        save_name2 = ca_name.replace(" ", "_") + '_catchment_data_long_' + dt.today().strftime('%m-%d-%Y') + '.xlsx'
        full_path = os.path.join(os.getcwd(), ca_dir, save_name)
        full_path2 = os.path.join(os.getcwd(), ca_dir, save_name2)

        with ExcelWriter(full_path, mode = 'w') as writer:
            print('Writing data to file...')
            pd.read_csv('CIFTools_Documentation.csv',
                        header = None, encoding = "ISO-8859-1").to_excel(writer, header = None,  #Zella note: why are we using this encoding?
                                                                         sheet_name = 'Variables and Sources', index = False)
            cdata['cancer_incidence'].to_excel(writer, sheet_name = 'Cancer Incidence', index = True)
            cdata['cancer_mortality'].to_excel(writer, sheet_name = 'Cancer Mortality', index = True)
            cdata['economy_county'].to_excel(writer, sheet_name = 'Economy (County)', index = False)
            cdata['economy_tract'].to_excel(writer, sheet_name = 'Economy (Tract)', index = False)
            cdata['environment_county'].to_excel(writer, sheet_name = 'Environment (County)', index = False)
            cdata['environment_tract'].to_excel(writer, sheet_name = 'Environment (Tract)', index = False)
            #cdata['broadband_speeds'].to_excel(writer, sheet_name = 'Broadband Speeds', index = False) #can be too long in some areas
            cdata['ht_county'].to_excel(writer, sheet_name = 'H and T (County)', index = False)
            cdata['ht_tract'].to_excel(writer, sheet_name= 'H and T (Tract)', index = False)
            cdata['rf_and_screening_county'].to_excel(writer, sheet_name= 'RF and Screening (County)', index=True)
            cdata['rf_and_screening_tract'].to_excel(writer, sheet_name= 'RF and Screening (Tract)', index=True)
            cdata['sociodemographics_county'].to_excel(writer, sheet_name = 'Sociodemographic (County)', index = False)
            cdata['sociodemographics_tract'].to_excel(writer, sheet_name = 'Sociodemographic (Tract)', index = False)
            cdata['facilities_and_providers'].to_excel(writer, sheet_name = 'Facilities', index = False)

        with ExcelWriter(full_path2, mode = 'w') as writer:
            print('Writing data to file...')
            pd.read_csv('CIFTools_Documentation.csv',
                        header = None, encoding = "ISO-8859-1").to_excel(writer, header = None,
                                                                         sheet_name = 'Variables and Sources', index = False)
            cdata['cancer_incidence_long'].to_excel(writer, sheet_name = 'Cancer Incidence', index = True)
            cdata['cancer_mortality_long'].to_excel(writer, sheet_name = 'Cancer Mortality', index = True)
            cdata['economy_county_long'].to_excel(writer, sheet_name = 'Economy (County)', index = False)
            cdata['economy_tract_long'].to_excel(writer, sheet_name = 'Economy (Tract)', index = False)
            cdata['environment_county_long'].to_excel(writer, sheet_name = 'Environment (County)', index = False)
            cdata['environment_tract_long'].to_excel(writer, sheet_name = 'Environment (Tract)', index = False)
            #cdata['broadband_speeds'].to_excel(writer, sheet_name = 'Broadband Speeds', index = False) # can be too long in some areas
            cdata['ht_county_long'].to_excel(writer, sheet_name = 'H and T (County)', index = False)
            cdata['ht_tract_long'].to_excel(writer, sheet_name= 'H and T (Tract)', index = False)
            cdata['rf_and_screening_county_long'].to_excel(writer, sheet_name= 'RF and Screening (County)',
                                                            index=True)
            cdata['rf_and_screening_tract_long'].to_excel(writer, sheet_name= 'RF and Screening (Tract)',
                                                          index=True)
            cdata['sd_county_long'].to_excel(writer, sheet_name = 'Sociodemographic (County)', index = False)
            cdata['sd_tract_long'].to_excel(writer, sheet_name = 'Sociodemographic (Tract)', index = False)
            cdata['facilities_and_providers'].to_excel(writer, sheet_name = 'Facilities', index = False)

        print(save_name + ' created')


    ### write data to CSVs
    def save_as_csvs(cdata:dict) -> None:
        from datetime import datetime as dt
        today = dt.today().strftime('%m-%d-%Y')

        os.chdir(path2)

        cdata['cancer_incidence'].to_csv(ca_name + '_cancer_incidence_county_' + today + '.csv', encoding='utf-8', index=True)
        cdata['cancer_mortality'].to_csv(ca_name + '_cancer_mortality_county_' + today + '.csv', encoding='utf-8', index=True)
        cdata['cancer_incidence_long'].to_csv(ca_name + '_cancer_incidence_county_long_' + today + '.csv', encoding='utf-8', index=True)
        cdata['cancer_mortality_long'].to_csv(ca_name + '_cancer_mortality_county_long_' + today + '.csv', encoding='utf-8', index=True)
        cdata['economy_county'].to_csv(ca_name + '_economy_county_' + today + '.csv', encoding='utf-8', index=False)
        cdata['economy_county_long'].to_csv(ca_name + '_economy_county_long_' + today + '.csv', encoding='utf-8', index=False)
        cdata['economy_tract'].to_csv(ca_name + '_economy_tract_' + today + '.csv', encoding='utf-8', index=False)
        cdata['economy_tract_long'].to_csv(ca_name + '_economy_tract_long_' + today + '.csv', encoding='utf-8', index=False)
        cdata['environment_county'].to_csv(ca_name + '_environment_county_' + today + '.csv', encoding='utf-8', index=False)
        cdata['environment_county_long'].to_csv(ca_name + '_environment_county_long_' + today + '.csv', encoding='utf-8', index=False)
        cdata['environment_tract'].to_csv(ca_name + '_environment_tract_' + today + '.csv', encoding='utf-8', index=False)
        cdata['environment_tract_long'].to_csv(ca_name + '_environment_tract_long_' + today + '.csv', encoding='utf-8', index=False)
        cdata['rf_and_screening_county'].to_csv(ca_name + '_rf_and_screening_county_' + today + '.csv', encoding='utf-8', index=True)
        cdata['rf_and_screening_tract'].to_csv(ca_name + '_rf_and_screening_tract_' + today + '.csv', encoding='utf-8', index=True)
        cdata['rf_and_screening_county_long'].to_csv(ca_name + '_rf_and_screening_county_long_' + today + '.csv', encoding='utf-8', index=True)
        cdata['rf_and_screening_tract_long'].to_csv(ca_name + '_rf_and_screening_tract_long_' + today + '.csv', encoding='utf-8', index=True)
        cdata['sociodemographics_county'].to_csv(ca_name + '_sociodemographics_county_' + today + '.csv', encoding='utf-8', index=False)
        cdata['sd_county_long'].to_csv(ca_name + '_sociodemographics_county_long_' + today + '.csv', encoding='utf-8', index=False)
        cdata['sociodemographics_tract'].to_csv(ca_name + '_sociodemographics_tract_' + today + '.csv', encoding='utf-8', index=False)
        cdata['sd_tract_long'].to_csv(ca_name + '_sociodemographics_tract_long_' + today + '.csv', encoding='utf-8', index=False)
        cdata['broadband_speeds'].to_csv(ca_name + '_broadband_speeds_' + today + '.csv', encoding='utf-8', index=False)
        cdata['facilities_and_providers'].to_csv(ca_name + '_facilities_and_providers_' + today + '.csv', encoding='utf-8', index=False)

        print('Success! CSVs created')


    # run compile and write functions
    cdata = comp_data()
    # save_as_xlsx(cdata = cdata) # Zella note: un-commenting-out this for testing
    save_as_csvs(cdata = cdata)
    comp_data4AGOL()
