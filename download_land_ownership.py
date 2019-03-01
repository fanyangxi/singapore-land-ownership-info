import requests
import time
import multiprocessing as mp
import re, os
import json

# access onemap-website using browser and copy the token from console:
onemap_api_token = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOjMsInVzZXJfaWQiOjMsImVtYWlsIjoicHVibGljQXBpUm9sZUBzbGEuZ292LnNnIiwiZm9yZXZlciI6ZmFsc2UsImlzcyI6Imh0dHA6XC9cL29tMi5kZmUub25lbWFwLnNnXC9hcGlcL3YyXC91c2VyXC9zZXNzaW9uIiwiaWF0IjoxNTUxMTYyNDY0LCJleHAiOjE1NTE1OTQ0NjQsIm5iZiI6MTU1MTE2MjQ2NCwianRpIjoiMmY2MGZhMDQ3Yjg0Yjc4MTY5YzY3NjcwMjkwZGMxODcifQ.GPcjMGj4mge0kPZxHbpgCeTo6hiqxa55iF1kkVMx8pM'
onemap_api_base_url = 'http://developers.onemap.sg/commonapi'
retrying_postal_codes_file = 'data-retrying-postal-codes.json'
result_landlots_file = 'data-result-landlots.json'

# result file operator:: write
def failure_result_listener(queue):
    f = open(retrying_postal_codes_file, 'w') 
    try:
        while 1:
            m = queue.get()
            if m == 'kill':
                f.write('killed')
                break
            f.write(str(m))
            f.flush()
    except Exception as e:
        print('Writing file failed, with error: {}'.format(e))
        raise
    finally:
        f.close()

def success_result_listener(queue):
    '''listens for messages on the queue, writes to file. '''
    f = open(result_landlots_file, 'w') 
    try:
        while 1:
            m = queue.get()
            if m == 'kill':
                f.write('killed')
                break
            f.write(str(m))
            f.flush()
    except Exception as e:
        print('Writing file failed, with error: {}'.format(e))
        raise
    finally:
        f.close()


def pcode_to_data(pcode, success_message_queue, failure_message_queue):
    regexMatch = re.match("..000\d", pcode)
    if regexMatch:
        print(pcode)
    # if int(pcode) % 1000 == 0:
    #     print(pcode)
    
    page = 1
    results = []

    while True:
        try:
            postCodeRetriveUrl = 'http://developers.onemap.sg/commonapi/search?searchVal={0}&returnGeom=Y&getAddrDetails=Y&pageNum={1}' \
                .format(pcode, page)
            postCodeResponse = requests.get(postCodeRetriveUrl).json()

            if postCodeResponse['found'] > 0:
                # print("Postal data retrived for {0}".format(pcode))
                postCodeLatitude = postCodeResponse['results'][0]['LATITUDE']
                postCodeLongtitude = postCodeResponse['results'][0]['LONGTITUDE']

                getLandownerInfoUrl = 'https://developers.onemap.sg/publicapi/landlotAPI/retrieveLandOwnership?latitude={0}&longtitude={1}&token={2}' \
                    .format(postCodeLatitude, postCodeLongtitude, onemap_api_token)
                # print(getLandownerInfoUrl)
                response = requests.get(getLandownerInfoUrl).json()
                results = results + response['LandOwnershipInfo']

            if postCodeResponse['totalNumPages'] > page:
                page = page + 1
                failure_message_queue.put('{0} has more than one page\n'.format(pcode))
            else:
                break

        except Exception as e:
            print('Fetching {0} failed. Skip and continue in 2 sec, with error: {1}'.format(pcode, e))
            failure_message_queue.put(pcode)

            time.sleep(2)
            continue

    if (len(results) > 0):
        jstr = json.dumps(results).encode('utf-8')
        # print(">>>> Putting: data-length-{0}".format(len(jstr)))
        success_message_queue.put(jstr)
    return results

if __name__ == '__main__':
    postal_codes = []

    ### Option-3:
    if os.path.exists(retrying_postal_codes_file):
        with open(retrying_postal_codes_file, 'r') as f:
            postal_codes = f.readlines()

    if len(postal_codes) == 0:
        ### Option-2:
        with open('data-sg-postal-codes.json', 'r') as f:
            postal_codes_data = json.load(f)
            print('Original count: {0}'.format(len(postal_codes_data['postalCodes'])))
            postal_codes = list(set(postal_codes_data['postalCodes']))
            print('Distincted count: {0}'.format(len(postal_codes)))

        ### Option-1:
        # postal_codes = range(10000, 820000)
        # postal_codes = ['{0:06d}'.format(p) for p in postal_codes]

    print('To process postal_codes count: {0}'.format(len(postal_codes)))

    #must use Manager queues here, or will not work
    manager = mp.Manager()
    success_message_queue = manager.Queue()
    failure_message_queue = manager.Queue()
    pool = mp.Pool(mp.cpu_count() + 2)

    # step-1.put listeners to work first
    watcher = pool.apply_async(success_result_listener, (success_message_queue,))
    watcher = pool.apply_async(failure_result_listener, (failure_message_queue,))

    # step-2: fire other workers
    jobs = []
    for postal_code in postal_codes:
        job = pool.apply_async(pcode_to_data, (postal_code, success_message_queue, failure_message_queue))
        jobs.append(job)

    # step-3: collect results from the workers through the pool result queue
    for job in jobs: 
        job.get()

    # step-3: now we are done, kill the listener
    success_message_queue.put('kill')
    failure_message_queue.put('kill')
    pool.close()

    # all_buildings = pool.map(pcode_to_data, postal_codes)
    # # all_buildings.sort(key=lambda b: (b['POSTAL'], b['SEARCHVAL']))
    # # jstr = json.dumps([y for x in all_buildings for y in x], indent=2, sort_keys=True)
    # with open('buildings-lot-info.json', 'w') as f:
    #     f.write(jstr.encode('utf-8'))

