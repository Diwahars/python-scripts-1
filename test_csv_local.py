import json,requests,csv,time
#from requests.auth import HTTPBasicAuth

INPUT_ADDRESS_FILE = "input.csv"
RESULT_ADDRESS_FILE = "output.csv"

HEADERS = {'Content-type': 'application/json', 'Accept': 'text/plain'}

URI='api.delhivery.io'

#CID_URL = 'http://{}/address/segment'.format(URI)
RESULTS_URL = 'http://127.0.0.1:5000/segment_address'

#AUTH = HTTPBasicAuth('xxxxx','xxxxxxx')

f = open(INPUT_ADDRESS_FILE)
reader = csv.reader(f)
keys = reader.next()

f2 = open(RESULT_ADDRESS_FILE, 'w')

l = ['wbn','add','pin','loc','loc_a','subloc','returned_pin','mismatch',
      'confidence','latitude','longitude']

writer = csv.DictWriter(f2, fieldnames = l)
writer.writeheader()

for row in reader:
    max_retry = 0
    payload = []
    record_dict = {}
    
    try:
        for i in range(len(row)):
            record_dict[keys[i]] = row[i]
        payload.append({"add":row[1], "pin":row[2]})
        """
        r = requests.post(CID_URL, data = json.dumps(payload), headers = HEADERS,
                        auth = AUTH)
        
        if r.status_code != 200:
            continue

        cid = r.json()
        """

        while max_retry < 5:
            r2 = requests.post(RESULTS_URL, data = json.dumps(payload),
                            headers = HEADERS)
            
            if r2.status_code == 200:
                res = r2.json()
                x = {}
                x['wbn'] = record_dict['wbn']
                x['add'] = record_dict['add']
                x['pin'] = record_dict['pin'],
                
                if 'locality' in res[0]:
                    x['loc']            = res[0]['locality']
                    x['loc_a']          = res[0]['locality_additional']
                    x['subloc']         = res[0]['sublocality']
                    x['returned_pin']   = res[0]['pin']
                    x['mismatch']       = res[0]['pin_loc_mismatch']
                    x['confidence']     = res[0]['confidence']['locality']
                    x['latitude']       = res[0]['latitude']
                    x['longitude']      = res[0]['longitude']
                
                writer.writerow(x)
                break
            else:
                max_retry += 1
                time.sleep(0.3)

    except Exception as err:
        print('Exception {} against request: {}'.format(err, payload))
        continue


f.close()
f2.close()

