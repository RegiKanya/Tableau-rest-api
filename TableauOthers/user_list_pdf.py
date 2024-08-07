# this script is downloading all the users from Tableau

from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils import querying
from fpdf import FPDF
from datetime import date


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
print("Connection status: signed in")

users_df = querying.get_users_dataframe(conn)
print(users_df)

font_color = (64, 64, 64)

pdf = FPDF()
pdf.add_page()
pdf.set_font('Arial', 'B', 8)

title = f"Active users from Tableau on {date.today().strftime('%m/%d/%Y')}"
pdf.cell(0, 20, title, align='C', ln=1)


for index, row in users_df.iterrows():
    # group_name = row['group', ['name']]
    user_name = row['name']
    display_name = row['fullName']
    user_email = row['email']
    site_status = row['siteRole']
    pdf.cell(0, 5, f"Name: {user_name}, Email: {user_email}, Status: {site_status}", ln=1)


pdf_file_name = '/Users/regina.kanya/Desktop/Tableau users/user_list.pdf'
pdf.output(pdf_file_name)