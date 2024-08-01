import configparser
import csv

from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils.querying import get_groups_dataframe, get_users_dataframe

file_path = 'existing-user-list.csv'

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


def remove_users(conn, users_df, email):
    # Get the user ID based on the provided email
    user_id = users_df.loc[users_df['email'].str.lower() == email.lower(), 'id'].values[0]

    # Get all groups and find the group ID for "External User"
    groups_df = get_groups_dataframe(conn)
    external_group_id = groups_df.loc[groups_df['name'] == 'External users', 'id'].values[0]

    # Remove the user from the "External User" group
    response = conn.remove_user_from_group(user_id=user_id, group_id=external_group_id)
    if response.status_code == 204:
        print(f"User with email {email} successfully removed from 'External User' group.")
    else:
        print(f"Failed to remove user with email {email} from 'External User' group.")
    return response


def update_user_and_add_to_group(conn, user):
    # Step 1: Check if the user exists
    users_df = get_users_dataframe(conn)
    existing_user = users_df[users_df['email'] == user["email"]]

    if existing_user.empty:
        print(f"User with email {user['email']} does not exist.")
        return None

    existing_user_id = existing_user.iloc[0]['id']

    # Step 2: Update user details if necessary
    response = conn.update_user(user_id=existing_user_id, new_email=user.get("email"), new_full_name=user.get("name"))

    # Step 3: Add the user to the new group(s)
    groups_df = get_groups_dataframe(conn)
    group_names = user.get("group", "")
    if isinstance(group_names, str):
        group_names = group_names.split(',')  # assuming groups are comma-separated in the CSV
    group_names = [name.strip() for name in group_names]
    group_ids = list(groups_df.loc[groups_df['name'].isin(group_names), 'id'])

    responses = []
    for group_id in group_ids:
        responses.append(conn.add_user_to_group(group_id=group_id, user_id=existing_user_id))

    response = conn.get_groups_for_a_user(user_id=existing_user_id)
    # print(response.json())
    return response


def main():
    conn = TableauServerConnection(tableau_server_config)
    conn.sign_in()
    cnt = 0
    with open(file_path) as csvfile:
        reader = csv.DictReader(csvfile)
        for user in reader:
            try:
                update_user_and_add_to_group(conn, user)
                cnt += 1
                users_df = get_users_dataframe(conn)
                remove_users(conn, users_df, user["email"])
            except Exception as e:
                print(f"{user['name']} not added due to error: {e}")
    print(f"{cnt} users added")


if __name__ == '__main__':
    main()
