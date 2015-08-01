import argparse

import json
import requests

HEADERS = {'Content-type': 'application/json', 'Accept': 'text/plain'}
URI = 'xxx.delhivery.io'


def load_data(file_path):
    '''
    Load json data from file
    '''
    fhandle = open(file_path, 'r')
    try:
        response = json.load(fhandle)
    except ValueError:
        raise ValueError(
            'Invalid input files. Please ensure that both '
            'test cases and validation are in json format')
    fhandle.close()
    return response


def dump_data(data, file_path):
    fhandle = open(file_path, 'w')
    json.dump(data, fhandle)
    fhandle.close()


def test_service(ipfile, validation_file, output_file):

    data = load_data(ipfile)

    # Fetch the CID
    print('Queueing the request')
    url = 'http://{}/address/segment'.format(URI)
    r = requests.post(url, data=json.dumps(data), headers=HEADERS)
    # check if the status code is correct
    assert(r.status_code == 200)

    data_primary = r.json()

    # Fetch the actual response
    print('Fetching response to queued request: {}'.format(data_primary))
    url = 'http://{}/address/results'.format(URI)
    r = requests.post(url, data=json.dumps(data_primary), headers=HEADERS)
    print(r.json())
    # check if the status code is correct
    assert(r.status_code == 200)

    try:
        response = r.json()
    except Exception as msg:
        raise('Unable to fetch json from request: {}'.format(msg))

    dump_data(response, output_file)

    type_check(data, response, validation_file)
    value_check(data, response, validation_file)

    print('Completed')


def type_check(primary_data, response, validation_file):
    '''
    Type check against output
    '''
    data = load_data(validation_file)
    assert(len(data) == len(response))

    for row_primary, row_data, row_response in zip(
            primary_data, data, response):
        response_keys = set(row_response.keys())
        data_keys = set(row_data.keys())

        try:
            assert(data_keys - response_keys == set())
        except AssertionError as msg:
            print('Request Data: {}'.format(row_primary))
            print('Expected Data: {}'.format(row_data))
            print('Response Data: {}'.format(row_response))

            raise AssertionError(
                'Response and data key mismatch: \n data_keys: {} '
                '\nresponse_keys: {}\n{}'.format(
                    data_keys, response_keys, msg))

        for key, value in row_data.items():
            assert(type(row_response[key]) == type(value))


def value_check(data, response, validation_file):
    '''
    Type check against output
    '''
    data = load_data(validation_file)
    assert(len(data) == len(response))

    for row_data, row_response in zip(data, response):
        response_keys = set(row_response.keys())
        data_keys = set(row_data.keys())

        assert(data_keys - response_keys == set())

        for key, value in row_data.items():
            try:
                assert(row_response[key] == value)
            except AssertionError:
                if type(value) == str:
                    assert(row_response[key].lower() == value.lower())
#                raise AssertionError(
#                    'Error in matching values: {} == {} {}'.format(
#                        row_response[key], value, msg))

if __name__ == '__main__':
    options = argparse.ArgumentParser()
    options.add_argument(dest='input_file')
    options.add_argument(dest='validation_file')
    options.add_argument(dest='output_file')

    config = options.parse_args()
    test_service(config.input_file, config.validation_file, config.output_file)
