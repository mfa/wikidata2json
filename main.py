import asyncio
import bz2
import glob
import json
import os

import aiofiles as aiof
import dateutil.parser
from lxml import etree, objectify


async def export_page(title, data):
    filename = f'output/{title}.json.bz2'
    if not os.path.exists(filename):
        async with aiof.open(filename, 'wb') as fp:
            compressed_data = bz2.compress(json.dumps(data, indent=4, sort_keys=True).encode())
            await fp.write(compressed_data)
            await fp.flush()


async def process_file_xml(filename, rename):
    for event, element in etree.iterparse(open(filename, 'rb'), tag='{*}page'):
        title = element.find('title', namespaces=element.nsmap).text
        if title.startswith(('Q', 'P')):
            revision = element.find('revision', namespaces=element.nsmap)
            data = revision.find('text', namespaces=element.nsmap).text
            # isotime = revision.find('timestamp', namespaces=element.nsmap).text
            # dt = dateutil.parser.parse(isotime)
            try:
                d = json.loads(data.replace('&quot;', '"'))
            except json.decoder.JSONDecodeError:
                print(f'Error in {title}')
            else:
                await export_page(title, d)
        del element
        del event
    if rename:
        os.rename(filename, filename + '.done')


async def process_file_json(filename, rename):
    async with aiof.open(filename) as fp:
        async for line in fp:
            if line.strip() in ['[', ']']:
                continue
            line = line.strip()
            line = line[:-1] if line[-1] == ',' else line
            data = json.loads(line)
            await export_page(data['id'], data)
    if rename:
        os.rename(filename, filename + '.done')


async def process(rename):
    for fn in glob.glob('input/*.xml'):
        await process_file_xml(fn, rename)
    for fn in glob.glob('input/*.json'):
        await process_file_json(fn, rename)


loop = asyncio.get_event_loop()
loop.run_until_complete(process(rename=False))
