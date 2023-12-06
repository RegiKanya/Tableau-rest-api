from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils import querying, flatten_dict_column
import configparser
import pandas as pd

file_path = 'user_removed.csv'

# define Tableau Server config details
config = configparser.ConfigParser()
config.read('tableau.ini')

token_name = config['tableau']['token_name']
personal_access_token = config['tableau']['personal_access_token']
server_url = config['tableau']['server_url']
api_version = config['tableau']['api_version']

config = {
    'tableau_prod': {
        'server': server_url,
        'api_version': api_version,
        'personal_access_token_name': token_name,
        'personal_access_token_secret': personal_access_token,
        'site_name': '',
        'site_url': ''
    }
}

# establish connection with Tableau Server
conn = TableauServerConnection(config)
conn.sign_in()
print("Connection status: signed in")

# get info about all users on the site
users_df = querying.get_users_dataframe(conn)
# print(users_df)

# get the user ID for the user being removed
with open('user_removed.csv', 'r') as t:
    users = pd.read_csv(t)

users_to_remove = users['email'].tolist()
for email_address in users_to_remove:
    user_id_to_remove = users[users['email'] == email_address]['id'].to_list().pop()
    print(f"User ID to remove: {user_id_to_remove}")

# get the user ID for the user who will take over the ownership
new_owner_id = (
    users_df[users_df['siteRole'].str.contains('Administrator')]['id']
    .to_list().pop()
)
print(f"New user ID to get ownership: {new_owner_id}")

# get all content from the user being removed
workbooks_df = querying.get_workbooks_dataframe(conn)
workbooks_df['content_type'] = 'workbook'
# print(workbooks_df)

datasources_df = querying.get_datasources_dataframe(conn)
datasources_df['content_type'] = 'datasource'

flows_df = querying.get_flows_dataframe(conn)
flows_df['content_type'] = 'flow'

content_dfs = [workbooks_df, datasources_df, flows_df]
content_ownership_df = pd.DataFrame()


# change ownership of all content which the user being removed owns
def change_content_owner(conn, content_type, content_id, new_owner_id):
    if content_type == 'workbook':
        response = conn.update_workbook(workbook_id=content_id, new_owner_id=new_owner_id)
    if content_type == 'datasource':
        response = conn.update_datasource(datasource_id=content_id, new_owner_id=new_owner_id)
    if content_type == 'flow':
        response = conn.update_flow(flow_id=content_id, new_owner_id=new_owner_id)
    return response


col_to_keep = ['id', 'name', 'content_type', 'owner_id']
for index, content_df in enumerate(content_dfs):
    if not content_df.empty:
        temp_df = content_df.copy()
        temp_df = flatten_dict_column(temp_df, keys=['id'], col_name='owner')
        temp_df = temp_df[col_to_keep]
        content_ownership_df = pd.concat([content_ownership_df, temp_df])

owner_swap_df = content_ownership_df[content_ownership_df['owner_id'] == user_id_to_remove]

for index, row in owner_swap_df.itertuples():
    response = change_content_owner(conn, row['content_type'], row['id'], new_owner_id)
    print(response.json())

# user no longer owns any content, it can be properly removed from the site
with open('user_removed.csv', 'r') as t:
    users = pd.read_csv(t)

for user in users:
    user_id = [0]
    conn.remove_user_from_site(user_id=user_id_to_remove)

# end the connection
conn.sign_out()