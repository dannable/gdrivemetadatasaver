import os
import csv
import argparse
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# Define the scope for the Google Drive API
SCOPES = ['https://www.googleapis.com/auth/drive']

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

def list_file_history(file_id):
    """Retrieve the entire version history for a specific file, including creation date."""
    creds = authenticate()
    service = build('drive', 'v3', credentials=creds)
    
    # Get the file details to retrieve the creation date and name
    response = service.files().get(
        fileId=file_id, 
        fields='name, createdTime',
        supportsAllDrives=True  # Added supportsAllDrives
    ).execute()
    file_name = response['name']
    created_time = response['createdTime']
    
    # Fetching revisions and including 'lastModifyingUser.displayName'
    versions_response = service.revisions().list(
        fileId=file_id, 
        fields='revisions(id, mimeType, modifiedTime, size, keepForever, published, lastModifyingUser)',
        supportsAllDrives=True  # Added supportsAllDrives
    ).execute()
    versions = versions_response.get('revisions', [])

    history = []
    for version in versions:
        history.append({
            'File ID': file_id,
            'File Name': file_name,
            'Creation Date': created_time,
            'Version ID': version.get('id', 'N/A'),
            'MIME Type': version.get('mimeType', 'N/A'),
            'Modified Time': version.get('modifiedTime', 'N/A'),
            'Size': version.get('size', 'N/A'),
            'Keep Forever': version.get('keepForever', 'N/A'),
            'Published': version.get('published', 'N/A'),
            'Last Modifying User': version.get('lastModifyingUser', {}).get('displayName', 'N/A')
        })
    
    return history

def save_metadata(metadata):
    """Save metadata to a CSV file."""
    with open('file_history.csv', 'w', newline='') as csvfile:
        fieldnames = [
            'File ID', 
            'File Name', 
            'Creation Date', 
            'Version ID', 
            'MIME Type', 
            'Modified Time', 
            'Size', 
            'Keep Forever', 
            'Published',
            'Last Modifying User'
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for data in metadata:
            writer.writerow(data)
    
    print('File history saved to file_history.csv')

def list_files_and_save_history(folder_id, is_shared_drive):
    """List files in a specific Google Drive or shared drive directory and save their history."""
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
            includeItemsFromAllDrives=True,  # Include items from all drives
            corpora='drive' if is_shared_drive else 'user',  # Specify corpora based on whether it's a shared drive
            driveId=folder_id if is_shared_drive else None,  # Set driveId if it's a shared drive
            supportsAllDrives=True,  # Added supportsAllDrives
            pageToken=page_token
        ).execute()
        
        for file in response.get('files', []):
            print(f"Processing file: {file['name']} (ID: {file['id']})")
            history = list_file_history(file['id'])
            all_history.extend(history)
        
        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break

    save_metadata(all_history)

def main():
    parser = argparse.ArgumentParser(description='Download metadata for files in a Google Drive or shared drive folder.')
    parser.add_argument('folder_id', help='The ID of the Google Drive or shared drive folder')
    parser.add_argument('--shared_drive', action='store_true', help='Specify if the folder is in a shared drive')
    args = parser.parse_args()
    
    list_files_and_save_history(args.folder_id, args.shared_drive)

if __name__ == '__main__':
    main()
