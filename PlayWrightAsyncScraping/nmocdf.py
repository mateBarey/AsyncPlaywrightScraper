from playwright.async_api import async_playwright
import pandas as pd 
import os
import asyncio 
from asyncio import gather, Semaphore, run, wait_for
import aiofiles 
from collections import defaultdict
import httpx

def chunks(lst,n):
    for i in range(0,len(lst),n):
        yield lst[i:i+n]
def mod_l(lst,n):
    if len(lst)%n == 0 :
        return [i for i in chunks(lst,n)]
    else:
        r = len(lst)%n 
        lst1 = lst[:(-r)]
        x = [i for i in chunks(lst1,n)]
        lst2 = lst[(-r):]
        return x + [lst2] 
    
class Crawlasync():
    '''
    Crawlasync is a crawler that can asynchronously download files
    from NMOCD using API numbers for oil wells in a csv

    :wellfile param: is path of csv file with a column of api numbers
    :path param: is the path to where you want the file to be saved
    :option param: Boolean if crawler should run headless or not
    '''
    def __init__(self,wellfile,path,option):
        self.wellfile = wellfile 
        self.path = path 
        self.option = option
        self.semaphore = Semaphore(3)
    def get_api_arr(self):
        '''
        get_api_arr function
        uses pandas to make the wellfile csv into a df
        then returns the list of the api columns with a string of 0000 added
        to each well api number 
        '''
        df = pd.read_csv(self.wellfile)
        wells = list(set(df.API.values.tolist()))
        wells = [str(int(i)) + '0000' for i in wells]
        wells = mod_l(wells,3)
        return wells 
    
    async def download_and_write(self,api,client):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.option)
            address = f'https://ocdimage.emnrd.nm.gov/imaging/WellFileView.aspx?RefType=WF&RefID={api}'
            page = await browser.new_page()
            await page.goto(address)
            links = await page.locator("//a[contains(@href, '.pdf')]").all()
            url_list = []
            for i,link in enumerate(links):
                link = await link.get_attribute('href')
                url_list.append(link)
            #return url_list 
            for i,url in enumerate(url_list):
                retries = defaultdict(int)
                while True:
                    name = f'{api}{i}.pdf'
                    res = await client.get(url)
                    content = res.read()
                    if res.status_code != 200:
                        retries[api] += 1
                        if retries[api] >3:
                            print(f'Download Failed for {api}!: {res.status_code}')
                            await asyncio.sleep(3)
                            break 
                        continue
                    os.chdir(self.path)
                    if os.path.exists(api):
                        os.chdir(api)
                    else:
                        os.mkdir(api)
                        os.chdir(api)
                    async with aiofiles.open(name,'wb') as f:
                        await f.write(content)
                        break 
            await asyncio.sleep(3)


    async def get_tasks(self):
        array_of_wells = self.get_api_arr()
        async with self.semaphore:
            async with httpx.AsyncClient() as client:
                for wells in array_of_wells:
                    tasks = [asyncio.create_task(self.download_and_write(api,client)) for api in wells]
                    await gather(*tasks)
    

if __name__ == '__main__':
    well_file = r"C:\Users\grc\Downloads\nmocd_wells.csv"
    path_f = r"C:\Users\grc\Downloads\apitest2"
    option = False

    async def main():
        nmocd_crawl = Crawlasync(wellfile=well_file,path=path_f,option=option)
        await nmocd_crawl.get_tasks()

    asyncio.run(main())