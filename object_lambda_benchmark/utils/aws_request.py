import io
import json
import os
import datetime, hashlib, hmac
import time

import aiocurl as pycurl


def sign(key, msg):
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def get_signature_key(key, date_stamp, regionName, serviceName):
    kDate = sign(('AWS4' + key).encode('utf-8'), date_stamp)
    kRegion = sign(kDate, regionName)
    kService = sign(kRegion, serviceName)
    kSigning = sign(kService, 'aws4_request')
    return kSigning


def get_amz_date():
    t = datetime.datetime.utcnow()
    amz_date = t.strftime('%Y%m%dT%H%M%SZ')
    return amz_date


def get_authorisation_header(method, canonical_uri, canonical_querystring, canonical_headers, signed_headers,
                             payload_hash, amz_date, region, service, aws_environment_variables):
    if aws_environment_variables is None:
        access_key = os.environ['AWS_ACCESS_KEY_ID']
        secret_key = os.environ['AWS_SECRET_ACCESS_KEY']
    else:
        access_key = aws_environment_variables['AWS_ACCESS_KEY_ID']
        secret_key = aws_environment_variables['AWS_SECRET_ACCESS_KEY']

    t = datetime.datetime.utcnow()
    date_stamp = t.strftime('%Y%m%d')  # Date w/o time, used in credential scope

    canonical_request = method + '\n' + canonical_uri + '\n' + canonical_querystring + '\n' + canonical_headers + '\n' + signed_headers + '\n' + payload_hash

    algorithm = 'AWS4-HMAC-SHA256'
    credential_scope = date_stamp + '/' + region + '/' + service + '/' + 'aws4_request'
    string_to_sign = algorithm + '\n' + amz_date + '\n' + credential_scope + '\n' + hashlib.sha256(
        canonical_request.encode('utf-8')).hexdigest()

    signing_key = get_signature_key(secret_key, date_stamp, region, service)

    signature = hmac.new(signing_key, (string_to_sign).encode('utf-8'), hashlib.sha256).hexdigest()

    authorization_header = algorithm + ' ' + 'Credential=' + access_key + '/' + credential_scope + ', ' + 'SignedHeaders=' + signed_headers + ', ' + 'Signature=' + signature

    return authorization_header


async def perform_request(url, headers, method, verbose, payload=""):
    if verbose:
        print('\nBEGIN REQUEST++++++++++++++++++++++++++++++++++++')
        print(f'Request URL = {url}')

    buffer = io.BytesIO()
    c = pycurl.Curl()
    if method == "POST":
        c.setopt(pycurl.POST, 1)
        if len(payload) != 0:
            c.setopt(pycurl.POSTFIELDS, payload)
    c.setopt(pycurl.URL, url)  # set url
    c.setopt(pycurl.FOLLOWLOCATION, 1)
    c.setopt(pycurl.WRITEFUNCTION, buffer.write)
    c.setopt(pycurl.HTTPHEADER, headers)

    await c.perform()  # execute
    # dns_time = c.getinfo(pycurl.NAMELOOKUP_TIME)  # DNS time
    # conn_time = c.getinfo(pycurl.CONNECT_TIME)  # TCP/IP 3-way handshaking time
    starttransfer_time = c.getinfo(pycurl.STARTTRANSFER_TIME)  # time-to-first-byte time
    total_time = c.getinfo(pycurl.TOTAL_TIME)  # last requst time
    response_code = c.getinfo(pycurl.RESPONSE_CODE)
    address = c.getinfo(pycurl.PRIMARY_IP)

    data = buffer.getvalue()
    body = data.decode("utf-8")
    c.close()
    if verbose:
        print('\nRESPONSE++++++++++++++++++++++++++++++++++++')
        print(f'TTFB: {starttransfer_time}. Body size: {len(data)}')
        print(f'Body (first 512 bytes): {body[:512]}\n')
    if response_code != 200:
        raise Exception(f"HTTP response code is not 200. Code is {response_code}")
    return {'body': body, 'ttfb': starttransfer_time, 'total_time': total_time, 'ip_address': address,
            'end_time': time.time()}


async def s3_get(bucket, key, region, aws_environment_variables=None, verbose=True):
    method = 'GET'
    service = 's3'
    host = f'{service}.{region}.amazonaws.com'
    canonical_uri = f'/{bucket}/{key}'
    content_type = 'application/octet-stream'
    payload_hash = hashlib.sha256("".encode('utf-8')).hexdigest()
    amz_date = get_amz_date()
    canonical_headers = 'content-type:' + content_type + '\n' + 'host:' + host + '\n' + 'x-amz-content-sha256:' + payload_hash + '\n' + 'x-amz-date:' + amz_date + '\n'
    signed_headers = 'content-type;host;x-amz-content-sha256;x-amz-date'
    canonical_querystring = ''
    authorization_header = get_authorisation_header(method, canonical_uri, canonical_querystring, canonical_headers,
                                                    signed_headers,
                                                    payload_hash, amz_date, region, service, aws_environment_variables)

    headers = [f'X-Amz-Date: {amz_date}',
               f'Authorization: {authorization_header}',
               f'Content-Type: {content_type}',
               f'X-Amz-Content-Sha256: {payload_hash}']

    url = f'https://{host}{canonical_uri}'
    return await perform_request(url, headers, method, verbose)


async def s3ol_get(bucket, key, region, params, aws_environment_variables=None, verbose=True):
    method = 'GET'
    service = 's3-object-lambda'
    host = f'{bucket}.{service}.{region}.amazonaws.com'
    canonical_uri = f'/{key}'
    content_type = 'application/octet-stream'
    payload_hash = hashlib.sha256("".encode('utf-8')).hexdigest()
    amz_date = get_amz_date()

    canonical_headers = 'content-type:' + content_type + '\n' + 'host:' + host + '\n' + 'x-amz-content-sha256:' + payload_hash + '\n' + 'x-amz-date:' + amz_date + '\n'
    signed_headers = 'content-type;host;x-amz-content-sha256;x-amz-date'

    user_headers = [('X-Function-Params', json.dumps(params))]

    for k, v in user_headers:
        canonical_headers += f'{k.lower()}:{v}\n'

    for k, _ in user_headers:
        signed_headers += f';{k.lower()}'

    canonical_querystring = ''
    authorization_header = get_authorisation_header(method, canonical_uri, canonical_querystring, canonical_headers,
                                                    signed_headers, payload_hash, amz_date, region, service,
                                                    aws_environment_variables)

    headers = [f'X-Amz-Date: {amz_date}',
               f'Authorization: {authorization_header}',
               f'Content-Type: {content_type}',
               f'X-Amz-Content-Sha256: {payload_hash}']

    for k, v in user_headers:
        headers.append(f'{k}: {v}')

    url = f'https://{host}{canonical_uri}'
    result = await perform_request(url, headers, method, verbose)
    return result


async def lambda_invoke(function_name, region, payload, aws_environment_variables=None, verbose=True):
    method = 'POST'
    service = "lambda"
    host = f'{service}.{region}.amazonaws.com'
    canonical_uri = f'/2015-03-31/functions/{function_name}/invocations'
    amz_date = get_amz_date()
    canonical_headers = 'host:' + host + '\n' + 'x-amz-date:' + amz_date + '\n'
    signed_headers = 'host;x-amz-date'

    if len(payload) != 0:
        payload = json.dumps(payload)

    payload_hash = hashlib.sha256(payload.encode('utf-8')).hexdigest()

    canonical_querystring = ''
    authorization_header = get_authorisation_header(method, canonical_uri, canonical_querystring, canonical_headers,
                                                    signed_headers, payload_hash, amz_date, region, service,
                                                    aws_environment_variables)

    headers = [f'X-Amz-Date: {amz_date}',
               f'Authorization: {authorization_header}',
               f'Content-Length: {len(payload)}']

    url = f'https://{host}{canonical_uri}'
    return await perform_request(url, headers, method, verbose, payload)
