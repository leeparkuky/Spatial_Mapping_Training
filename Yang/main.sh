#!/bin/bash

# python3 -c "import chromedriver_autoinstaller; chromedriver_autoinstaller.install(path = './input_folder')"


user_id='adelineliyang'  # user_name
#password='JL2Fgr5FI9bIJ7nI'   client_id
password='0802150529Aa'
gis_address='https://ky-cancer.maps.arcgis.com'
file_path='dataset.pickle'

python3 testing_arcgis.py -i $user_id -p $password -g $gis_address -f $file_path