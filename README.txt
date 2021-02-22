### JIRA TICKETS - 2020 WEEKLY HISTORIC LOAD
#
# This script is used to extract, transform and load weekly Jira ticket snapshots into a Power BI OneDrive repostiory
# This script accounts for Jira's inability to extract business-ready historic views of its issue types without transformation
#
# In order to create weekly snapshots, the script works as follows
    # 1) User Jira Service Account username, password and domain requested 
    # 2) User Jira domain URL requested
    # 3) For each week of 2020, a base table will be created accounting for all tickets in Jira created by the end of that week (JQL/ Jira Query) as a point of reference OR last port of call for field value (see below)
    # 4) The changelog for this Jira Query will then be extracted and transformed into an intermediate table and used to populate every field for every ticket in the snapshot
    # 5) Three scenarios exist for a field on a ticket:
        # i) A ticket field has only changed directly BEFORE the created date - USE CHANGELOG 'Before_Value'
        # ii) A ticket field has only changed directly AFTER the created date - USE CHANGELOG 'After_Value'
        # iii) A ticket field has NEVER been changed and therefore does not appear in the changelog - BYPASS CHANGELOG, USE BASE TABLE VALUE
        # iv) A ticket field has only been RAISED with a value but never changed - BYPASS CHANGELOG, USE BASE TABLE VALUE
    # 6) Resulting tables are merged together to create a snapshot view
    # 7) Table is specifically-named, converted to xlsx. and sent to Sharepoint
    # 8) Steps 1-7 repeated until all week-end snapshots completed
#
### IMPORTANT NOTE:
# This script is run directly through the Power BI Python Plug-in
# Whilst use of the plug-in offers speedier and more efficient ETL process from inside Power BI, the plug-in does not recognise the use of functions
# For this reason, this script is intentionally written in a continuous block without being formatted with functions

 