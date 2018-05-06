from urllib.parse import urljoin
import requests
from lxml.html import fromstring
import asyncio
import aiohttp
import random
import csv
import re
import time


class KansasSocialWorkers:
    """
    Gather information on licenses publicly available at https://www.kansas.gov/bsrb-verification/

    """

    def __init__(self):
        self.base_url = 'https://www.kansas.gov/bsrb-verification/search/results?' \
                        'searchTerm=s&lastName={lname}&firstName={fname}&licenseNumber={lic_num}&offset=0&max=100000'
        self.output_filename = 'Kansas_social_workers.csv'
        self.list_rows = '//table/tr'
        self.list_scraper = (
            ('link', './td[1]/a', '@href'),
        )

        self.detail_xpath = '//table/tr[{}]/td[2]/strong'
        self.detail_scraper = (
            ('ENTITY_NAME_FULL', self.detail_xpath.format(1), 'text'),
            ('LICENSE_0_ID', self.detail_xpath.format(3), 'text'),
            ('LICENSE_0_TYPE', self.detail_xpath.format(4), 'text'),
            ('ADDRESS_0_CITY', self.detail_xpath.format(2), 'text'),
            ('LICENSE_0_END_DATE', self.detail_xpath.format(5), 'text'),
            ('LICENSE_0_BEGIN_DATE', self.detail_xpath.format(6), 'text'),
            ('LICENSE_0_QUALIFIER', self.detail_xpath.format(7), 'text'),
            ('LICENSE_0_STATUS', self.detail_xpath.format(8), 'text'),
            ('LICENSE_0_DISCIPLINE', self.detail_xpath.format(9), 'text'),
        )
        self.output_headers = [item[0] for item in self.detail_scraper]
        self.out_file = open(self.output_filename, 'w')
        self.writer = csv.DictWriter(self.out_file, fieldnames=self.output_headers)
        self.writer.writeheader()

        self.sem = asyncio.Semaphore(5)
        self.tasks = []
        self.headers = {
            'Connection': 'keep-alive',
            'Pragma': 'no-cache',
            'Cache-Control': 'no-cache',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9',
        }

        self.proxies = {'http': 'http://us-il.proxymesh.com:31280',
                        'https': 'http://us-il.proxymesh.com:31280'}

    async def get(self, url, avg_sleep=2, proxy=None):
        """asyncronous http GET request"""

        async with self.sem:
            print('sleeping... ' + str(url))
            await asyncio.sleep(random.uniform(abs(avg_sleep - 1), avg_sleep + 1))
            async with aiohttp.ClientSession(headers=self.headers) as session:
                async with session.get(url, proxy=proxy) as resp:
                    return await resp.text()

    async def gather_details(self, item):
        """Asyncronous function for getting detail page and parsing it for relevant data"""

        detail_page = await self.get(urljoin(self.base_url, item.get('link','').strip('.')))
        if 'An error has occurred' in detail_page:
            raise Exception('bad detail page')

        record = self.scrape(fromstring(detail_page), self.detail_scraper)
        self.writer.writerow(record)
        print('gathered record for', record['ENTITY_NAME_FULL'])

    def scrape(self, tree, scraper):
        """Used to parse html page using xpath queries stored in the 'scraper' variable"""
        record = {}
        for label, xpath, data_type in scraper:
            items = tree.xpath(xpath)
            if len(items) == 0:
                item_text = ''
            else:
                if len(items) > 1:
                    raise Exception('more than one item found for given xpath for field label: {}'.format(label))

                item = items[0]

                if re.match(r'(?:text)$', data_type, flags=re.I):
                    item_text = item.text
                elif re.match(r'@(.*)$', data_type):
                    attrname = re.match(r'@(.*)$', data_type).group(1)
                    item_text = item.get(attrname, '')
                else:
                    raise Exception("WARNING: Unknown dataType specified: {}".format(data_type))

            record[label] = item_text.strip() if item_text else ''
        return record

    def search(self, lname='', fname='', lic_num=''):
        """search website for provided criteria and return results"""
        s = requests.session()
        # session.proxies = self.proxies
        results_response = None
        num_retries = 0
        max_retries = 5
        while not results_response or results_response.status_code != requests.codes.ok:
            if num_retries > 0:
                print('failed to load results, retrying search')
            if num_retries >= max_retries:
                raise Exception('search retried {} times. Scrape failed to load results.'.format(max_retries))

            time.sleep(random.uniform(5, 10))
            results_response = s.get(self.base_url.format(lname=lname, fname=fname, lic_num=lic_num))
            num_retries += 1
        tree = fromstring(results_response.text)
        records = []
        for item in tree.xpath(self.list_rows):
            records.append(self.scrape(item, self.list_scraper))
        return records

    async def gather_results(self, todo_results, max_retries=10):
        """use asyncronous requests to gather all links provided in the todo_results variable"""
        num_retries = 0
        while len(todo_results) > 0:
            if num_retries > max_retries:
                raise Exception('too many retries')
            if num_retries > 0:
                print('error gathering results. retrying. attempt #', num_retries)

            tasks = []
            for item in todo_results:
                tasks.append(asyncio.ensure_future(self.gather_details(item)))

            results = await asyncio.gather(*tasks, return_exceptions=True)
            todo_results = [todo_results[i] for i, item in enumerate(results) if
                            item and issubclass(type(item), Exception)]
            num_retries += 1

    def run(self, queries=[]):
        """
        Gather all results for search criteria in 'queries' and write output to file.


        :param queries: list of dictionaries with search criteria. lic_num, fname, and lname are acceptable criteria
        """
        loop = asyncio.get_event_loop()
        try:
            for q in queries:
                results = self.search(**q)
                loop.run_until_complete(self.gather_results(results))
        finally:
            self.out_file.close()
            for task in asyncio.Task.all_tasks():
                task.cancel()
            loop.run_until_complete(asyncio.ensure_future(asyncio.sleep(0)))
            asyncio.get_event_loop().close()


if __name__ == '__main__':
    scraper = KansasSocialWorkers()
    scraper.run(queries=[
        {'lname':'Agee'},
        {'lic_num':'0898'}
    ])
