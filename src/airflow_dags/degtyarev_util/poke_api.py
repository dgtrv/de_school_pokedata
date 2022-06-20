"""
Module for accessing API asynchronously.

Async restrictions should be defined here.
"""
import asyncio
import time
from typing import Callable, List, Union

import aiohttp

MAX_THREADS_FOR_API_CALLS = 2
MAX_API_CALLS_PER_SECOND_PER_THREAD = 5
DELAY_PER_TASK = 1.0 / MAX_API_CALLS_PER_SECOND_PER_THREAD


async def _gather_with_concurrency(maximum_threads, *tasks):
    semaphore = asyncio.Semaphore(maximum_threads)

    async def sem_task(task):
        async with semaphore:
            return await task

    return await asyncio.gather(*(sem_task(task) for task in tasks))


async def _get_async(
    url: str,
    session: aiohttp.ClientSession,
    save_results_to: Union[Callable, dict],
    processing: List[Callable] = None
):
    async with session.get(url) as response:
        print(f"time: {time.time()}, calling url: {url}")
        obj = await response.json()
        print(f"time: {time.time()}, got url: {url}")

        if processing:
            for function in processing:
                obj = function(obj)

        await asyncio.sleep(DELAY_PER_TASK)

        if type(save_results_to) is dict:
            save_results_to[url] = obj
            return

        save_results_to(obj)


async def make_api_requests(
    save_results_to_or_with: Union[Callable, dict],
    urls: List[str] = [],
    processing: List[Callable] = None,
):
    """
    Make a request, process and save result for each supplied URL.

    :param save_results_to_or_with: function or dictionary for saving
    results; if it's a dictionary it will be filled with result data, in
    case of function it will be applied to every result entry
    :param urls: URLs for making requests
    :param processing: list of optional processing functions
    """
    session = aiohttp.ClientSession()

    await _gather_with_concurrency(
        MAX_THREADS_FOR_API_CALLS,
        *[
            _get_async(url, session, save_results_to_or_with, processing)
            for url in urls
        ],
    )

    await session.close()


async def get_pokeapi_endpoint(
    endpoint_url: str,
    save_results_with: Callable,
    processing: List[Callable] = None,
) -> None:
    """
    Get all the entries from API endpoint.

    Get endpoint elements count, then get all the elements,
    apply optional processing functions consequently to all the entries,
    save results with supplied saving function

    :param endpoint_url: full API endpoint URL
    :param save_results_with: function that will be applied to every
    result entri for saving it
    :param processing: optinal processing functions
    """
    if not endpoint_url.endswith("/"):
        endpoint_url = f"{endpoint_url}/"

    endpoint_info = {}

    await make_api_requests(endpoint_info, [endpoint_url])

    endpoint_count = int(endpoint_info[endpoint_url]["count"])

    endpoint_list = {}

    await make_api_requests(
        endpoint_list, [f"{endpoint_url}?limit={endpoint_count}"]
    )

    urls_to_request = []

    for element in endpoint_list[f"{endpoint_url}?limit={endpoint_count}"][
        "results"
    ]:
        urls_to_request.append(element["url"])

    await make_api_requests(
        save_results_to_or_with=save_results_with,
        urls=urls_to_request,
        processing=processing,
    )
