import os
import csv
import argparse
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# Define the scope for the Google Drive API
SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly']

def authenticate():
    """Authenticate and create the API client."""
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return creds

def list_file_history(service, file_id):
    """Retrieve the entire version history for a specific file."""
    versions = []
    
    # Get the file details to retrieve the version history
    file_metadata = service.files().get(fileId=file_id, fields='name').execute()
    file_name = file_metadata['name']
    
    revisions_response = service.revisions().list(fileId=file_id, fields='revisions(id, mimeType, modifiedTime, size, keepForever, published)').execute()
    revisions = revisions_response.get('revisions', [])

    history = []
    for revision in revisions:
        history.append({
            'File ID': file_id,
            'File Name': file_name,
            'Version ID': revision.get('id', 'N/A'),
            'MIME Type': revision.get('mimeType', 'N/A'),
            'Modified Time': revision.get('modifiedTime', 'N/A'),
            'Size': revision.get('size', 'N/A'),
            'Keep Forever': revision.get('keepForever', 'N/A'),
            'Published': revision.get('published', 'N/A')
        })
    
    return history

def save_metadata(metadata):
    """Save metadata to a CSV file."""
    with open('file_history.csv', 'w', newline='') as csvfile:
        fieldnames = ['File ID', 'File Name', 'Version ID', 'MIME Type', 'Modified Time', 'Size', 'Keep Forever', 'Published']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for data in metadata:
            writer.writerow(data)
    
    print('File history saved to file_history.csv')

def list_files_and_save_history(folder_id):
    """List files in a specific Google Drive directory and save their history."""
    creds = authenticate()
    service = build('drive', 'v3', credentials=creds)
    query = f"'{folder_id}' in parents"
    
    page_token = None
    all_history = []
    
    while True:
        response = service.files().list(
            q=query,
            spaces='drive',
            fields='nextPageToken, files(id, name)',
            pageToken=page_token
        ).execute()
        
        for file in response.get('files', []):
            print(f"Processing file: {file['name']} (ID: {file['id']})")
            history = list_file_history(service, file['id'])
            all_history.extend(history)
        
        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break

    save_metadata(all_history)

def main():
    parser = argparse.ArgumentParser(description='Download metadata for files in a Google Drive folder.')
    parser.add_argument('folder_id', help='The ID of the Google Drive folder')
    args = parser.parse_args()
    
    list_files_and_save_history(args.folder_id)

if __name__ == '__main__':
    main()
