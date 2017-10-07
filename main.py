import asyncio
import glob
import json

import aiofiles as aiof
import dateutil.parser
from lxml import etree, objectify


async def export_page(title, data, dt):
    async with aiof.open(f'output/{title}.json', 'w') as fp:
        await fp.write(json.dumps(json.loads(data.replace('&quot;', '"'))))
        await fp.flush()


async def process_file(filename):
    for event, element in etree.iterparse(open(filename, 'rb'), tag='{*}page'):
        title = element.find('title', namespaces=element.nsmap).text
        if title.startswith(('Q', 'P')):
            revision = element.find('revision', namespaces=element.nsmap)
            data = revision.find('text', namespaces=element.nsmap).text
            isotime = revision.find('timestamp', namespaces=element.nsmap).text
            dt = dateutil.parser.parse(isotime)
            await export_page(title, data, dt)


async def process():
    for fn in glob.glob('input/*'):
        await process_file(fn)


loop = asyncio.get_event_loop()
server = loop.run_until_complete(process())
