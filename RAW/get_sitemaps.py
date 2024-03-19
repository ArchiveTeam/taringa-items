import multiprocessing.pool
import multiprocessing.dummy
import os
import re
import typing

import requests


def get_and_dump(url: str, return_data: bool = True,
                 directory: str = 'sitemaps') -> typing.Optional[str]:
    print(url)
    while True:
        try:
            response = requests.get(url, timeout=5)
            break
        except Exception:
            print('retrying', url)
            continue
    assert len(response.content) > 0 and response.status_code == 200
    with open(os.path.join(directory, url.rsplit('/', 1)[1]), 'wb') as f:
        f.write(response.content)
    if return_data:
        return response.text
        
        
def get_sitemap(url: str, pool: multiprocessing.pool.ThreadPool):
    get_url = lambda item: re.search('<loc>(.+?)</loc>', item).group(1).strip()
    data = get_and_dump(url)
    discovered_items = set()
    pool.starmap(get_sitemap, [
        (get_url(item), pool)
        for item in re.findall('<sitemap>(.+?)</sitemap>', data)
    ])
    for item in re.findall('<url>(.+?)</url>', data):
        newurl = get_url(item)
        channel = re.search(r'^https?://(?:www\.)?taringa\.net/\+([^/]+)', newurl)
        if channel:
            discovered_items.add('channel:'+channel.group(1))
        story = re.search(r'^https?://(?:www\.)?taringa\.net/[^/]+/.+?_([0-9a-z]{1,6})$', newurl)
        if story:
            discovered_items.add('story:'+story.group(1))
    if len(discovered_items) > 0:
        with open(os.path.join('sitemap_items', url.rsplit('/', 1)[1]+'.items'), 'w') as f:
            f.write('\n'.join(discovered_items)+'\n')


def main():
    for directory in ('sitemaps', 'sitemap_items'):
        if not os.path.isdir(directory):
            os.makedirs(directory)
    robotstxt = get_and_dump('https://www.taringa.net/robots.txt')
    with multiprocessing.dummy.Pool(100) as p:
        for line in robotstxt.splitlines():
            if not line.startswith('Sitemap:'):
                continue
            get_sitemap(line.split(':', 1)[1].strip(), p)

if __name__ == '__main__':
    main()

