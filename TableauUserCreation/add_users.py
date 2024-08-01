import configparser
import csv

from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils.querying import get_groups_dataframe

file_path = 'users.csv'

# import configs
config = configparser.ConfigParser()
config.read('tableau.ini')

token_name = config['tableau']['token_name']
personal_access_token = config['tableau']['personal_access_token']
server_url = config['tableau']['server_url']
api_version = config['tableau']['api_version']
site_name = config['tableau']['site_name']

tableau_server_config = {
    'tableau_prod': {
        'server': server_url,
        'api_version': api_version,
        'personal_access_token_name': token_name,
        'personal_access_token_secret': personal_access_token,
        'site_name': site_name,
        'site_url': ''
    }
}


def create_user(conn, user):
    response = conn.add_user_to_site(user_name=user["email"], site_role=user["license"], auth_setting="OpenID")
    new_user_id = response.json()['user']['id']
    response = conn.update_user(user_id=new_user_id,
                                new_email=user["email"],
                                new_full_name=user["name"])
    groups_df = get_groups_dataframe(conn)
    group_names = [user["group"]]
    group_ids = list(groups_df.loc[groups_df['name'].isin(group_names), 'id'])

    responses = []
    for group_id in group_ids:
        responses.append(conn.add_user_to_group(group_id=group_id, user_id=new_user_id))
    response = conn.get_groups_for_a_user(user_id=new_user_id)
    print(response.json())
    return response


def main():
    conn = TableauServerConnection(tableau_server_config)
    conn.sign_in()
    cnt = 0
    with open(file_path) as csvfile:
        reader = csv.DictReader(csvfile)
        for user in reader:
            try:
                create_user(conn, user)
                cnt += 1
            except:
                print(user['name'] + " ( email: " + user['email'] + ") not added")
    print(str(cnt) + ' users added')


if __name__ == '__main__':
    main()