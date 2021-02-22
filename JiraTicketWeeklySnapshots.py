from datetime import date, timedelta
import datetime as dt
import calendar
import pandas as pd
from jira.client import JIRA
from atlassian import Jira
from dateutil import parser
import numpy as np

############################################### Dataframe parameters in terminal ######################################
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 150)

########################################### JIRA REST API CONNECTION PARAMS ###########################################

FILE_ID = input("Please enter your log-in username: ") # User log-in
un = input("Please enter your Service Account username: ") # User SA input
pwd = input("Please enter your Service Account password: ") # User SA pw input
domain = input("Please enter the full URL of your domain i.e. https://jira.devops.domain.com: ") # Firm domain
options = {'server': f'{domain}'} 
jira = Jira(url = domain, username = un,password = pwd,) # Jira REST API connection

########################################### Date parameters ###########################################################

start_date = date(2020,2,23) # Starting date
end_date = date.today() # Current date
delta = timedelta(days=7) # Skip a week

weeks_ = [] # List to hold all Sundays from start of 2020-present

while start_date <= end_date:

    weeks_.append(start_date.strftime("%Y-%m-%d")) # Add Sunday date to list
    print(start_date.strftime("%Y-%m-%d") + " snapshot mark added for ingestion")
    start_date += delta # Skip to following week when added

########################################### JIRA API Connection ########################################################

# Test Jira connection if running this script in the terminal (debugging)

try:
    print('Connecting to Jira API')
    jira        

except: 
    print("Connection to JIRA API failed")

else:
    print('Successfully connected to JIRA API')

for week in weeks_:

    query_string = f"Project = JPS AND Issuetype = Initiative and Trajectory = 'Inbound' AND created <= {week}" # Live JQL query for tickets to be pulled and built to

    # Date for archive - e.g. last day of week - INPUT DESIRED DATE HERE [ArchiveDatetime_str]
    ArchiveDatetime_str = f'{week} 23:59:59'  # Datetime format as string
    ArchiveDate = datetime.strptime(week, '%Y-%m-%d') # Date format
    ArchiveDatetime = datetime.strptime(ArchiveDatetime_str, '%Y-%m-%d %H:%M:%S') # Datetime format

    # Date"Previous Year", "Current Year" and "Indicative Cost" were first created in JPS
    CY_IC_PY_CreatedDate = datetime.strptime('2020-10-15', '%Y-%m-%d') # used as conditional param. to improve performance - these fields only existed post-October 2020


    ############################################## INITIATIVES BASE TABLE ##############################################
    # This block will extract, transform and load a temporary table of the Initiatives in the Jira query as they appear at the time of executing this script
    # These values will be merged at the end with the table constructed from the changelog as previously mentioned
    #   - in the "Initiatives Changelog Table", "NULL" values occur because the field has never experienced a change and therefore does not register in the JIRA Changelog 
    #   - any "NULL" values in the "Initiatives Changelog Table" will be replaced with the values of that ticket as they appear today from the "Initiatives Base Table"


    # Base Initiatve table params
    issues_per_query = 100 # Pull and ingestion cycle size
    list_of_jira_issues = [] # Base Initiative table pre-DataFrame list initialisation

    # Get the total issues in the results set. This is one extra request but it keeps things simple.
    num_issues_in_query_result_set = jira.jql(query_string, limit = 100, fields=[
        "assignee", # Assignee 
        "key",
        "created", # Created
        #"customfield_13000", #Trajectory
        "summary", # Summary
        "customfield_10001", # Points 
        "customfield_15035", # Division 
        "customfield_23104", # Value Stream 
        "customfield_47218", # NRP 
        "customfield_15111", # Project Code 
        "customfield_33333", # Quote Status
        "customfield_33221", # RAG Status
        "customfield_77321", # Response Text
        "status", # Status
        "customfield_28462", # Estimated Total Cost
        "customfield_27789", #Previous Year
        "customfield_37895", # Current Year
        "customfield_81233" # Indicative Cost
        ])["total"]
    print(f"Query `{query_string}` returns {num_issues_in_query_result_set} issues")

    # Use floor division + 1 to calculate the number of requests needed
    # This 'for-loop' bypasses Jira's 1000 ticket REST API limit
    for query_number in range(0, (num_issues_in_query_result_set // issues_per_query) + 1):

        results = jira.jql(query_string, limit = issues_per_query, start = query_number * issues_per_query, fields=[
        "assignee", # Assignee
        "key",
        "created", # Created
        #"customfield_13000", #Trajectory
        "summary", # Summary
        "customfield_10001", # Points
        "customfield_15035", # Division
        "customfield_23104", # Value Stream
        "customfield_47218", # NRP
        "customfield_15111", # Project Code
        "customfield_33333", # Quote Status
        "customfield_33221", # RAG Status
        "customfield_77321", # Response Text
        "status", # Status
        "customfield_28462", # Estimated Total Cost
        "customfield_27789", #Previous Year
        "customfield_37895", # Current Year
        "customfield_81233"]) # Indicative Cost
        print("100 tickets ingested")
        list_of_jira_issues.extend(results["issues"])

    PY_Initiatives = pd.json_normalize(list_of_jira_issues) # Create dataframe from appended list

    # Define which fields we care about using dot notation for nested fields

    FIELDS_OF_INTEREST = [
        "fields.assignee.name", # Assignee
        "key",
        "fields.created", # Created
        #"fields.customfield_13000[0].value", #Trajectory
        "fields.summary", # Summary
        "fields.customfield_10001.value", # Points
        "fields.customfield_15035.value", # Division
        "fields.customfield_23104.value", # Value Stream
        "fields.customfield_47218", # NRP Code
        "fields.customfield_15111", # Project Code
        "fields.customfield_33333.value", # Quote Status
        "fields.customfield_33221.value", # RAG Status
        "fields.customfield_77321", # Response Text
        "fields.status.name", # Status
        "fields.customfield_28462", # Estimated Total Cost
        "fields.customfield_27789", #Previous Year
        "fields.customfield_37895", # Current Year
        "fields.customfield_81233" # Indicative Cost
    ]

    PY_Initiatives = PY_Initiatives[FIELDS_OF_INTEREST] # Filter to only display the fields we care about. To actually filter them out use df = df[FIELDS_OF_INTEREST].

    PY_Initiatives = PY_Initiatives.rename(columns = {
        "fields.assignee.name" : "assignee", # Assignee
        "fields.created" : "Created",
        #"fields.customfield_13000[0].value" : "Trajectory", #Trajectory
        "fields.summary" : "Summary",
        "fields.customfield_10001.value" : "Points", # Points
        "fields.customfield_15035.value" : "Division", # Division
        "fields.customfield_23104.value" : "Value Stream", # Value Stream
        "fields.customfield_47218" : "NRP Code", # NRP Code
        "fields.customfield_15111" : "Project Code", # Project Code
        "fields.customfield_33333.value" : "Quote Status", # Quote Status
        "fields.customfield_33221.value" : "RAG Status", # RAG Status
        "fields.customfield_77321" : "Response Text", # Response Text
        "fields.status.name" : "status", # Status
        "fields.customfield_28462" : "Estimated Total Cost", # Estimated Total Cost
        "fields.customfield_27789" : "Previous Year", # Previous Year
        "fields.customfield_37895" : "Current Year", # Current Year
        "fields.customfield_81233" : "Indicative Cost" # Indicative Cost
    })

    ############################################## INITIATIVES CHANGELOG TABLE ##############################################

    # This table is constructed by calling the API using the Jira query, expanding the returning changelog and re-constructing a snapshot through transforming the changelog entries into a readable table
    # The next step involves transforming the readable changelog table into the same format as a typical Jira csv export which mirrors an export on a particular date (i.e. last day of the week)

    pd.options.mode.chained_assignment = None  # default='warn'

    # Block parameters
    init_block_size = 50
    init_start_block = 0
    Initiatives_inserted = 0

    # JIRA REST API credentials
    jira_CL = JIRA(options, basic_auth=(un, pwd))           

    # Pre-DataFrame list initialisation
    list_of_jira_issues = []
    Initiatives_inserted = 0

    # Only bring "Current Year", "Indicative Cost" and "Previous Year" post-Oct 2020 to save space
    if (ArchiveDate < CY_IC_PY_CreatedDate):
        JiraFields = {"assignee", "Points", "Division", "Value Stream", "NRP Code", "Project Code", "Quote Status", "RAG Status", "Response Text", "status", "Estimated Total Cost"} 
    else:
        JiraFields = {"assignee", "Points", "Division", "Value Stream", "NRP Code", "Project Code", "Quote Status", "RAG Status", "Response Text", "status", "Estimated Total Cost", "Previous Year", "Current Year", "Indicative Cost"}
 
    # Initiative Changelog - JIRA REST API and ingestion
    while True: # Access live JIRA Changelog in this statement

        Initiatives = jira_CL.search_issues(query_string,startAt=init_start_block,maxResults=init_block_size,expand='changelog') # Call JIRA REST API for changelog up to ArchiveDate

        if init_start_block== 0: # Start run 
            print(f'Extracting {init_block_size} Initiatives via Rest API')
        for issue in Initiatives: # loop to read 50 record attriutes and insert into table

            print(f'{issue} ingested')

            Initiatives_inserted += 1
            if issue.changelog:
                num_changes = issue.changelog.total # group of changes API. One changelog item can have more than one change, created by the same author on the same date

            if num_changes>0:

                list_changes = issue.changelog.histories

                for change_item_group in list_changes:

                    #key variables, same across a group of field changes
                    Date_Of_Change = parser.parse(change_item_group.created).replace(tzinfo=None) # Date Of Change - strip timezone
                    Change_Creator_Name = change_item_group.author.displayName # Change creator name
                    Change_Creator_ID = change_item_group.author.name # Change creator file ID

                    Days_from_archive_date = abs(Date_Of_Change - ArchiveDatetime) # Abs value used to highlight youngest relevant change

                    for change_item in change_item_group.items:
                        Change_Field = change_item.field # Changed field
                        Before_Value = change_item.fromString # Value before change
                        After_Value = change_item.toString # Value after change

                        if Change_Field in JiraFields:                       
                            if(Date_Of_Change > ArchiveDatetime): 
                                list_of_jira_issues.append([issue.key, Date_Of_Change, Change_Creator_Name, Change_Creator_ID, Days_from_archive_date, "Increm", Change_Field, Before_Value, After_Value])
                            else:             
                                list_of_jira_issues.append([issue.key, Date_Of_Change, Change_Creator_Name, Change_Creator_ID, Days_from_archive_date, "Delta", Change_Field , Before_Value, After_Value])

        print(f'Total percentage of Initiative histories ingested: {"{:.0%}".format((Initiatives_inserted /Initiatives.total))}')

        # increment start_block
        init_start_block += init_block_size

        print(f'Initiatives ingested: ~{init_start_block}')
        if init_start_block > Initiatives.total : # > Initiatives.total:
            break # Kill run when we ingest all changelog

    # DataFrame creation using list created in above while statement
    PY_Initiatives_Changelog = pd.DataFrame(list_of_jira_issues, columns = ["key", "Date_Of_Change", "Change_Creator_Name", "Change_Creator_ID", "Days Until Archive Date","Change_Type", "Change_Field", "Before_Value", "After_Value"])  
    PY_Initiatives_Changelog['Date_Of_Change'] = pd.to_datetime(PY_Initiatives_Changelog['Date_Of_Change'])
    PY_Initiatives_Changelog.sort_values(by=["key", "Days Until Archive Date","Change_Field"], inplace=True, ascending = True)
    PY_Initiatives_Changelog = PY_Initiatives_Changelog.groupby(["key","Change_Field"]).head(1).reset_index(drop = True)
 
    #Delta tickets have experienced a change to a field right before the ArchiveDate; Increm tickets have experienced one right after the ArchiveDate
    PY_Initiatives_Changelog['Value_Test'] = np.where(PY_Initiatives_Changelog['Change_Type']=='Delta', PY_Initiatives_Changelog['After_Value'], PY_Initiatives_Changelog['Before_Value'])
    PY_Initiatives_Changelog = PY_Initiatives_Changelog.pivot(index = 'key', columns=["Change_Field"], values = ["Value_Test"])
    PY_Initiatives_Changelog = PY_Initiatives_Changelog.xs('Value_Test', axis=1,drop_level=True) # kill "Value_Test" index header
    ArchivedDF = pd.DataFrame(PY_Initiatives[["key", "Summary", "Created"]]) # Total tickets in snapshot to be involved in triple-join

    DF_Name = f'{ArchiveDate.date()} - JPS Initiatives Snapshot.xlsx' # Dynamic table / dataframe/ Excel file name
    DF_Name_ = str(DF_Name) # Convert dynamic name to string

    ######################################### INITIATIVES BASE & CHANGELOG TABLES TRIPLE-MERGE ##############################################

    DF_Merge = ArchivedDF.merge(PY_Initiatives_Changelog, on = 'key', how = 'left') # left-join
    DF_Merge = DF_Merge.fillna(PY_Initiatives) # Merge with current Initiative Base table - combine Delta & Increm values with Base table to fill in for the field values that slipped through the Changelog net
    DF_Merge['Archive Date/ EoW'] = ArchiveDate # Append column holding date of archive

    # Dataframes for removal - memory restoration

    DFs = [PY_Initiatives, PY_Initiatives_Changelog, ArchivedDF]
    del PY_Initiatives
    del PY_Initiatives_Changelog
    del ArchivedDF

    DF_Merge.to_excel(f'C:\\PBI\\EoW Archives - Initiatives\\{DF_Name_}') # Export to Excel on your computer
    DF_Merge.to_excel(f'C:\\Users\\{FILE_ID}\\OneDrive\\Weekly Archive\\{DF_Name_}') # Export Excel to Sharepoint

