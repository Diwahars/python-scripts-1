# Get data from the promptcloud servers (XML / JSON based)

import xml.etree.ElementTree as ET
import requests
import shutil
import os.path
from requests.auth import HTTPBasicAuth

url = 'https://api.promptcloud.com/data/info?id=delhivery_xxxxxxxx&from_date=20150401'
filename = 'pc_data/pc_data.xml'

print "Downloading XML into " + filename
req = requests.get(url, stream = True)
if req.status_code != 200:
    print "http error code " + req.status_code
else:
    # everything is fine
    # write file
    with open(filename, 'wb') as out_file:
            shutil.copyfileobj(req.raw, out_file)
    print "Parsing XML " + filename
    root = ET.parse(filename)
    for child in root.findall('entry'):
        u = child.find('url').text
        r = requests.get(u, auth = HTTPBasicAuth('delhivery_xxxxxxx', 'xxxxxx'),
                         stream = True)
        
        file_name = 'pc_data/' + u.split('/')[-1]
        if os.path.isfile(file_name):
                print file_name + " File exists ... skipping"
        else:
            print "Downloading file " + file_name
            with open(file_name, 'wb') as f:
                for chunk in r.iter_content(chunk_size = 1024):
                    if chunk:
                        f.write(chunk)
                        f.flush()

