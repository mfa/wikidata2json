import glob
import json

import dateutil.parser
from lxml import etree, objectify


def export_page(title, data, dt):
    with open(f'output/{title}.json', 'w') as fp:
        json.dump(json.loads(data.replace('&quot;', '"')), fp)


def process_file(filename):
     for event, element in etree.iterparse(open(filename, 'rb'), tag='{*}page'):
         title = element.find('title', namespaces=element.nsmap).text
         if title.startswith(('Q', 'P')):
             revision = element.find('revision', namespaces=element.nsmap)
             data = revision.find('text', namespaces=element.nsmap).text
             isotime = revision.find('timestamp', namespaces=element.nsmap).text
             dt = dateutil.parser.parse(isotime)
             export_page(title, data, dt)


for fn in glob.glob('input/*'):
    process_file(fn)
