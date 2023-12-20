# TableauUserDeletion
This script is used for deleting Tableau users in bulk. 

This script helps to delete users in bulk.
This method is based on https://towardsdatascience.com/how-to-remove-tableau-server-users-who-own-content-bb84fbd6948d 
article. 

What you need to do: 
1. refresh the email list in user_removed.csv file
2. run the user_deletion script

Logic: 
1. connect to the Tableau server
2. get the information about the user being removed
3. get a new user id if a content-ownership change is needed
4. check that there is a content on the user being removed
5. if it is then change the ownership
6. delete all the users who are listed in user_removed.csv


Challenges for the Tableau REST API:
- cannot change thr project ownership
- cannot change Privat collections ownership
