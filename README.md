# DMA - Automation of Output Comparison of DMA_Legacy and DMA_Modernized
These scripts demonstrates the Automation of output comparison for DMA_Legacy and DMA_Modernized and writes output files to s3 bucket.

List of endpoints used for Automation are listed below.

1.https://test.sta-rms.siemens-energy.cloud/api/v4/types/description/DMA Launcher - Test/  

2.https://test.sta-rms.siemens-energy.cloud/api/v4.5/Assemblies/userpackages  

3.https://test.sta-rms.siemens-energy.cloud/api/v4.5/Assemblies/27140/tree

4.https://test.sta-rms.siemens-energy.cloud/api/v4.5/tags/tagtypes

5.https://test.sta-rms.siemens-energy.cloud/api/v4.5/tags/?DatasourceId=135873

6.https://test.sta-rms.siemens-energy.cloud/api/v4.5/signals/data?TagIds=1,2&From=2024-02-18T00%3A00%3A00&To=2024-02-19T00%3A00%3A00&Timeout=60

7.https://test.sta-rms.siemens-energy.cloud/api/v4.5/Events?packageId=27140&from=2023-01-20T00:00:00&to=2024-01-20T23:59:59&tagIdentifierOption=TagName

8.https://test.sta-rms.siemens-energy.cloud/api/v4.5/agentmessages/daterange?assemblyId=27140&from=2023-09-01T00:00:00&to=2023-09-22T00:00:00&searchUsingCreationTime=true&timeout=60

9.https://test.sta-rms.siemens-energy.cloud/api/v4.5/agentmessages/daterange?assemblyId={package_id}&from={from_date}&to={to_date}&SearchUsingCreationTime=true"

## Steps to execute the Script
1. Configuration
	1. open dma-legacy-modernized-comparison-scripts\conf\product_assembly_conf.csv file
	2. Mark "Yes" under "enabled" column for those assemblies which you want to run the test script and "No" for those assemblies which you donot want to run the test script. Save and close the file
	3. open dma-legacy-modernized-comparison-scripts\conf\app.conf file
	4. under [DATE_RANGE] section app.conf file set the desired from_date and to_date value. 
	5. under [TOKEN] section in app.conf file set the latest bearer_token taken from STARMS-test page. 
2. Running the script
	1. open your desired IDE (Visual Code, Pycharm etc.)
	2. setup virtual environment (optional) and activate venv
		```
		python -m venv venv
		.\venv\Scripts\activate
		```
	3. Install necessary python packages. 
		```
		pip install -r requirements.txt
		```
	3. execute main.py function
		```
		python main.py
		```

## Script Working flow
1. Deletes DMA Launcher Data:
	Script deletes Signal and Event Tag ID values for Legacy and Modernized DMA Launcher. 
2. Checks if tasks are open:
	After data has been deleted, script checks for any open tasks in DMA Legacy and Modernized Launcher. If there are open tasks in STARMS-Test, then it first closes all the task and then re-opens the tasks for given from_date and to_date range. 
3. Monitors Task Status: 
	Once tasks are re-opened in STARMS-Test the script waits for all the tasks to get closed in both Legacy and Modernized DMA launcher in a while loop. 
2. Generates Signal, Event and Agent Message Data:
	Once all the tasks are closed, script hits GET API endpoints to generate signal, event and agent message data and stores it as .json file under dma-legacy-modernized-comparison-scripts\input_json folder	
	Data Generation flow is as follows:
	1. The Script fetches the launcher id based on launcher type and package id's from the assemblies listed in the wiki for testing.
	2. Using launcher id and package ids, Assemblyid(combination of launcher and assembly) is obtained. 
	3. Signal typeid's, Event typeid's are obtained based on the Signaltype and Eventtype. Replacing Assemblyid(combination of launcher and assembly) in the endpoint 5, Taglist is obtained(combination of signals and events).
	4. Signal Tag ids and Event Tag ids are retrieved from taglist after filtering based on the signal and event typeid's where Signal typeid's should match the Signal tagids and Event typeid's should match the Event tagids.
	5. The Signal Tagids's are replaced in the endpoint 6 and Signal data is obtained. 
	6. For event data, replacing package id in the endpoint 7, event data is obtained and replacing package id in endpoint 8 provides the agent messages.
	7. Agent message data is generated using endpoint 9 listed above.
	8. These data are written to two separate json files. One for Legacy and one for Modernized. These ouput json files are written to s3 specific folder.

5. Comparison of Legacy and Modernized Data. 
	Once json files are generated and uploaded to S3, the script fetches the latest generated file from the s3 bucket and executes comparision logic. The script compares 
	1. Compares Package_names and Product_line if present in both legacy and modernized DMA data. 
	2. Compares signal tag_id 'values' between legacy and modernized for given ['siteTime', 'tagName', 'tagAlias']
	3. Compares Event tag_id 'values' between legacy and modernized for given ['siteTime', 'timeStamp', 'eventCategory', 'tagName']. Ignores those event date where tagName is either {'Data Received', "Events Received"}
	4. Compares Agent Message data values for following parameters:- ['siteEventTime','messageText','messageClass','messageSeverity','messageScope']
	5. Uploads the comparison output to s3 bucket. 

The above scenario is performed for both DMA-Legacy and DMA-Modernized.

![alt text](https://code.siemens-energy.com/spd-ibs/dma/dma-legacy-modernized-comparison-scripts/-/raw/main/Working_flow.jpg?ref_type=heads)

## Pre-requisites to setup this script on your machine
1. Visual Code
2. python 3.10 and above
4. Bearer Token
5. AWS cli (to authenticate with S3)


