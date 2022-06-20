import asyncio
import time
from typing import Callable, List, Union

import aiohttp

MAX_THREADS_FOR_API_CALLS = 2
MAX_API_CALLS_PER_SECOND_PER_THREAD = 5
DELAY_PER_TASK = 1.0/MAX_API_CALLS_PER_SECOND_PER_THREAD


async def gather_with_concurrency(n, *tasks):
    semaphore = asyncio.Semaphore(n)

    async def sem_task(task):
        async with semaphore:
            return await task

    return await asyncio.gather(*(sem_task(task) for task in tasks))


async def get_async(url, session, save_results_to, processing: List[Callable] = None):
    async with session.get(url) as response:
        print(f'time: {time.time()}, calling url: {url}')
        obj = await response.json()
        print(f'time: {time.time()}, got url: {url}')
        if processing:
            for function in processing:
                obj = function(obj)
        await asyncio.sleep(DELAY_PER_TASK)
        if type(save_results_to) is dict:
            save_results_to[url] = obj
            return
        save_results_to(obj)


async def make_api_requests(save_results_to_or_with: Union[Callable, dict], urls: List[str] = [], processing: List[Callable] = None):
    session = aiohttp.ClientSession()

    conc_req = MAX_THREADS_FOR_API_CALLS
    await gather_with_concurrency(conc_req, *[get_async(i, session, save_results_to_or_with, processing) for i in urls])

    await session.close()



async def get_pokeapi_endpoint(endpoint_url: str, save_results_with: Callable, filter: Callable = None, processing: List[Callable] = None) -> None:
    '''Get endpoint elements count, then get all the elements'''
    if not endpoint_url.endswith('/'):
        endpoint_url = f'{endpoint_url}/'

    endpoint = {}

    await make_api_requests(endpoint, [endpoint_url])

    endpoint_count = int(endpoint[endpoint_url]['count'])

    endpoint_list = {}

    await make_api_requests(endpoint_list, [f'{endpoint_url}?limit={endpoint_count}'])

    urls_to_request = []

    for element in endpoint_list[f'{endpoint_url}?limit={endpoint_count}']['results']:
        urls_to_request.append(element['url'])

    await make_api_requests(save_results_to_or_with=save_results_with, urls=urls_to_request, processing=processing)

