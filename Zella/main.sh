#!/bin/bash



user_id='zch250@uky.edu' # user name for AGOL
password='aBYdELneCf6cnT2' # password for AGOL
gis_address='https://ky-cancer.maps.arcgis.com'
file_path='Dartmouth_catchment_data/dataset.pickle'


python3 testing_arcgis.py -i $user_id -p $password -g $gis_address -f $file_path
