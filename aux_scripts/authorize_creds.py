#!/usr/bin/env python
# coding: utf-8

# In[4]:


#this file runs through the Oauth authorization.  If it doesn't work, it could be because your credentials are out of date.
#delete the current credentials file (which is 'authorizedcreds.dat' by default) and re-run the script.

#imports
import httplib2
from googleapiclient.discovery import build
from oauth2client.client import OAuth2WebServerFlow
import argparse
from oauth2client import client
from oauth2client import file
from oauth2client import tools


#constants and default values
CLIENT_SECRETS_PATH = ''#PATH TO client_secrets.json you downloaded from API manager
# Variable parameter that controls the set of resources that the access token permits.
SCOPES = ['https://www.googleapis.com/auth/webmasters.readonly'] 
AUTHORIZED_CREDS_FILE = 'authorizedcreds.dat' #once you do oauth, this file should exist I think

#function to create credentials using Oauth2
def authorize_creds(creds: str = CLIENT_SECRETS_PATH, authorized_creds_file : str=AUTHORIZED_CREDS_FILE ):
    #creds should be CLIENT_SECRETS_PATH unless the user submits other credentials
 
    # Create a parser to be able to open browser for Authorization
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        parents=[tools.argparser])
    flags = parser.parse_args([])
 
    flow = client.flow_from_clientsecrets(
        creds, scope = SCOPES, #used to be CLIENT_SECRETS_PATH instead of creds
        message = tools.message_if_missing(creds)) #this used to be CLIENT_SECRETS_PATH
 
    # Prepare credentials and authorize HTTP
    # If they exist, get them from the storage object
    # credentials will get written back to a file.
    storage = file.Storage(authorized_creds_file)
    credentials = storage.get()
 
    # If authenticated credentials don't exist, open Browser to authenticate
    if credentials is None or credentials.invalid:
        credentials = tools.run_flow(flow, storage, flags)

    
    http = credentials.authorize(http=httplib2.Http())
    webmasters_service = build('webmasters', 'v3', http=http)
    return webmasters_service
 

