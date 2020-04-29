#!/usr/bin/env python3

import json
import os.path
import pickle
import time

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request


NUM_REQUESTS = 100
# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']


def process_response(service, response):
    texts = []

    def process_msg(_, response, exception):
        if exception is not None:
            print(response['nextPageToken'], exception)
            sleep(60)  # FIXME: urllib retry sleep
        else:
            headers = response.get('payload', {}).get('headers', [])
            for header in headers:
                if header['name'] == 'From':
                    fromtext = header['value']
                    texts.append(fromtext)

    if 'messages' in response:
        messages = response['messages']

        batch = service.new_batch_http_request()

        for message in messages:
            batch.add(service.users().messages().get(userId='me', id=message['id']), callback=process_msg)

        batch.execute()

    return texts


def main():
    """Shows basic usage of the Gmail API.
    Lists the user's Gmail labels.
    """
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # NB: Note that the URL displayed needs to be opened in Safari!
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('gmail', 'v1', credentials=creds)

    # Call the Gmail API to fetch INBOX
    texts = []

    print('Getting messages...')

    response = {
        # 'nextPageToken': None,
        # 'nextPageToken': '06940125780758342532',
    }

    while 'nextPageToken' in response:
        page_token = response['nextPageToken']

        response = service.users().messages().list(
            userId='me',
            pageToken=page_token,
            maxResults=NUM_REQUESTS,
        ).execute()
        response_texts = process_response(service, response)
        texts.extend(response_texts)
        time.sleep(5)  # FIXME: urllib retry sleep

        filename = f'batches/{page_token}_batch.json'
        print(f'\t{len(texts)} messages processed; saving {len(response_texts)} to {filename}...', end='', flush=True)

        with open(filename, 'w') as outfile:
            json.dump(texts, outfile)

        print('; saved.')


if __name__ == '__main__':
    main()
