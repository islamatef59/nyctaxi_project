import os
import shutil
import urllib.request

def download_file(url:str,dir_path:str,local_path:str):
   
   # Open a connection and stream the remote file
   response = urllib.request.urlopen(url)
   os.makedirs(dir_path, exist_ok=True)
    # Save the streamed content to the local file in binary mode
   with open(local_path, 'wb') as f:
    shutil.copyfileobj(response, f) 