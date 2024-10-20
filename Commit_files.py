import os
import base64
import requests

# Replace these variables with your own GitHub information
GITHUB_TOKEN = 'GITHUB-TOKEN'  # Your GitHub PAT
REPO_OWNER = 'REPOOWNER'
REPO_NAME = 'REPONAME'
BRANCH = 'main'  # The branch where you want to upload the file

# GitHub API URL
GITHUB_API_URL = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/"

# Function to delete a file from GitHub
def delete_file_from_github(file_name, sha):
    url = GITHUB_API_URL + file_name
    headers = {
        'Authorization': f'token {GITHUB_TOKEN}',
        'Content-Type': 'application/json'
    }
    
    # Prepare the payload for deletion
    payload = {
        'message': f'Delete {file_name}',
        'sha': sha,
        'branch': BRANCH
    }
    
    # Make the API call to delete the file
    response = requests.delete(url, json=payload, headers=headers)
    
    # Print detailed information for debugging
    if response.status_code == 200:
        print(f'Successfully deleted {file_name} from GitHub.')
    else:
        print(f'Failed to delete {file_name}. Status Code: {response.status_code}')
        print(f'Response JSON: {response.json()}')

# Function to retrieve existing files in the repository
def get_repo_contents():
    url = GITHUB_API_URL
    headers = {
        'Authorization': f'token {GITHUB_TOKEN}'
    }
    
    # Make the API call to retrieve the repository contents
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f'Failed to retrieve repository contents. Status Code: {response.status_code}')
        print(f'Response JSON: {response.json()}')
        return None

# Function to upload a file to GitHub
def upload_file_to_github(file_path):
    with open(file_path, 'rb') as file:
        content = file.read()
    
    # Encode the content to base64 as required by the GitHub API
    encoded_content = base64.b64encode(content).decode('utf-8')
    
    # Get the file name from the file path
    file_name = os.path.basename(file_path)
    
    # Prepare the URL and the headers for the API call
    url = GITHUB_API_URL + file_name
    headers = {
        'Authorization': f'token {GITHUB_TOKEN}',
        'Content-Type': 'application/json'
    }
    
    # Prepare the payload
    payload = {
        'message': f'Upload {file_name}',
        'content': encoded_content,
        'branch': BRANCH
    }
    
    # Make the API call to upload the file
    response = requests.put(url, json=payload, headers=headers)
    
    # Print detailed information for debugging
    if response.status_code == 201:
        print(f'Successfully uploaded {file_name} to GitHub.')
    else:
        print(f'Failed to upload {file_name}. Status Code: {response.status_code}')
        print(f'Response JSON: {response.json()}')
        print(f'URL: {url}')

# Function to upload all CSV and PY files in the current directory
def upload_all_files():
    for file_name in os.listdir('.'):
        if file_name.endswith('.csv') or file_name.endswith('.py'):
            upload_file_to_github(file_name)

# Main script logic
if __name__ == "__main__":

    # Step 1: Retrieve all files in the repository and delete the CSV and PY files
    repo_contents = get_repo_contents()
    if repo_contents:
        for file_info in repo_contents:
            file_name = file_info['name']
            if file_name.endswith('.csv') or file_name.endswith('.py'):
                # Delete the file from GitHub
                delete_file_from_github(file_name, file_info['sha'])
    
    # Step 2: Upload all CSV and PY files from the local directory
    upload_all_files()
