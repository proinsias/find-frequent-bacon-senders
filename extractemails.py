#!/usr/bin/env python3

import collections
import glob
import json
import re

import tqdm

# https://gist.github.com/dideler/5219706
EMAIL_REGEX = re.compile(("([a-z0-9!#$%&'*+\/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+\/=?^_`"
                    "{|}~-]+)*(@|\sat\s)(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(\.|"
                    "\sdot\s))+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)"))
FROM_TEXTS_FILE = 'texts.json'


def main():
    texts = []

    print(f'Loading from batch files...')
    for file in tqdm.tqdm(glob.glob('batches/*.json')):
        with open(file, 'r') as json_file:
            filetexts = json.load(json_file)
            texts.extend(filetexts)

    print(f'Saving texts to {FROM_TEXTS_FILE}...')
    with open(FROM_TEXTS_FILE, 'w') as outfile:
        json.dump(texts, outfile)

    print(f'Loading pre-aggregated texts from {FROM_TEXTS_FILE}...')
    with open(FROM_TEXTS_FILE, 'r') as json_file:
        texts = json.load(json_file)

    print(f'Extracting email addresses...')
    emails = []
    emailerrors = set()

    for fromtext in tqdm.tqdm(texts):
        try:
            fromemail = re.search(EMAIL_REGEX, fromtext.lower()).group(0)
            emails.append(fromemail)
        except AttributeError as err:
            emails.append(fromtext.lower())
            emailerrors.add(str(err))

    print(f'Email errors:\n{emailerrors}')

    filename = 'emails.json'
    print(f'Saving emails to {filename}')
    with open(filename, 'w') as outfile:
        json.dump(emails, outfile)

    filename = 'emails.json'
    print(f'Loading pre-processed emails from {filename}...')
    with open(filename, 'r') as json_file:
        emails = json.load(json_file)

    print(f'Extracting domains...')
    domains = []
    domainerrors = set()

    for fromemail in tqdm.tqdm(emails):
        try:
            atsplits = fromemail.split('@')
            fulldomain = atsplits[-1]
            dotsplits = fulldomain.split('.')
            fromdomain = '.'.join([dotsplits[-2], dotsplits[-1]])
            domains.append(fromdomain)
        except Exception as err:
            domainerrors.add(str(err))

    print(f'Domain errors:\n{domainerrors}')

    filename = 'domains.json'
    print(f'Saving domains to {filename}')
    with open(filename, 'w') as outfile:
        json.dump(emails, outfile)

    filename = 'emails.json'
    print(f'Loading pre-processed emails from {filename}...')
    with open(filename, 'r') as json_file:
        emails = json.load(json_file)

    filename = 'emails_dict.json'
    print(f'Saving email count dictionary to {filename}')
    emails_dict = collections.Counter(emails)
    with open(filename, 'w') as outfile:
        json.dump(emails_dict, outfile)

    filename = 'domains.json'
    print(f'Loading pre-processed domains from {filename}...')
    with open(filename, 'r') as json_file:
        domains = json.load(json_file)

    filename = 'domains_dict.json'
    print(f'Saving domain count dictionary to {filename}')
    domains_dict = collections.Counter(domains)
    with open(filename, 'w') as outfile:
        json.dump(domains_dict, outfile)

    # jq . emails_dict.json | sort -k 2,2 -n -r | more


if __name__ == '__main__':
    main()
