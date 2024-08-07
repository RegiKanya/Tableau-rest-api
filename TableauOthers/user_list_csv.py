import configparser
import os

import pandas as pd
from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils import extract_pages

file_path = '/Users/regina.kanya/Desktop/user_list_csv.csv'

config = configparser.ConfigParser()
config.read('tableau.ini')

config = {
    'tableau_prod': {
        'server': 'https://eu-west-1a.online.tableau.com/',
        'api_version': '3.22',
        'personal_access_token_name': 'RegiToken2',
        'personal_access_token_secret': 'aCFFWVPdTvCANGb4NFuq2Q==:1bhHRTsDkX2tKIjfMpg43DGkdfITlHHB',
        'site_name': 'axdata',
        'site_url': ''
    }
}

tableau_server = 'https://eu-west-1a.online.tableau.com/'
api_version = '3.22'
personal_access_token_name = 'RegiToken2'
personal_access_token_secret = 'aCFFWVPdTvCANGb4NFuq2Q==:1bhHRTsDkX2tKIjfMpg43DGkdfITlHHB'
site_name = 'axdata'

conn = TableauServerConnection(config)
conn.sign_in()
print('signed in')
users_on_site = extract_pages(conn.get_users_on_site, parameter_dict={'fields': 'fields=_all_'})
users_on_site_df = pd.DataFrame(users_on_site)
# try:
#     os.remove(file_path)
# except:
#     None
users_on_site_df.to_csv(file_path, index=False)
conn.sign_out()