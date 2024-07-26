import logging
from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils import querying, flatten_dict_column
import configparser
import pandas as pd

file_path = 'user_removed.csv'
config_file = 'tableau.ini'

# define Tableau Server config details
config = configparser.ConfigParser()
config.read(config_file)

config = {
    'tableau_prod': {
        'server': config['tableau']['server_url'],
        'api_version': config['tableau']['api_version'],
        'personal_access_token_name': config['tableau']['token_name'],
        'personal_access_token_secret': config['tableau']['personal_access_token'],
        'site_name': config['tableau']['site_name'],
        'site_url': ''
    }
}

# establish connection with Tableau Server
conn = TableauServerConnection(config)
conn.sign_in()
logging.info("Connection status: signed in")


def get_content_ownership(conn):
    content_df = [
        querying.get_projects_dataframe(conn).assign(content_type='project'),
        querying.get_workbooks_dataframe(conn).assign(content_type='workbook'),
        querying.get_datasources_dataframe(conn).assign(content_type='datasource'),
        querying.get_flows_dataframe(conn).assign(content_type='flow')
    ]

    content_ownership_df = pd.concat(content_df, ignore_index=True)
    return flatten_dict_column(content_ownership_df, keys=['id'], col_name='owner')[['id', 'name', 'content_type', 'owner_id']]


# change ownership of all content which the user being removed owns
def change_content_owner(conn, content_type, content_id, l_new_owner_id):
    if content_type == 'project':
        return "Cannot update project owners via REST API (yet)."
    if content_type == 'workbook':
        return conn.update_workbook(workbook_id=content_id, new_owner_id=l_new_owner_id)
    if content_type == 'datasource':
        return conn.update_datasource(datasource_id=content_id, new_owner_id=l_new_owner_id)
    if content_type == 'flow':
        return conn.update_flow(flow_id=content_id, new_owner_id=l_new_owner_id)
    return "Invalid content type."


# changing the ownership and removing the user
def remove_user(conn, users_df, email):
    try:
        user_id = users_df.loc[users_df['email'].str.lower() == email.lower(), 'id'].values[0]
        response = conn.remove_user_from_site(user_id=user_id)
        if response.status_code == 204:
            print(f"User with email {email} removed successfully.")
        else:
            print(f"Failed to remove user with email {email}. They may have content that needs reassignment.")
    except IndexError:
        logging.error(f"No user found with email {email}.")
    except Exception as ex:
        logging.error(f"An error occurred while removing user with email {email}: {ex}")


def main():
    try:
        # Get info about all users on the site
        users_df = querying.get_users_dataframe(conn)

        # Read users to be removed
        users_to_remove = pd.read_csv(file_path)['email'].str.lower().tolist()

        # Get new owner ID (an Administrator)
        new_owner_id = users_df.loc[users_df['siteRole'].str.contains('Administrator'), 'id'].values[0]

        for email in users_to_remove:
            user_id_to_remove = users_df.loc[users_df['email'] == email, 'id'].values[0]
            owner_swap_df = get_content_ownership(conn)
            owner_swap_df = owner_swap_df[owner_swap_df['owner_id'] == user_id_to_remove]

            for row in owner_swap_df.itertuples():
                response = change_content_owner(conn, row.content_type, row.id, new_owner_id)
                if hasattr(response, 'status_code') and response.status_code == 200:
                    logging.info(f"Content ownership changed for {row.content_type} ID {row.id}")
                else:
                    logging.error(f"Failed to change ownership for {row.content_type} ID {row.id}")

            remove_user(conn, users_df, email)
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
    finally:
        conn.sign_out()
        logging.info("Connection closed.")


if __name__ == '__main__':
    main()
