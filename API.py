def API_get_batchs():


def asyncs(urls):
    start_time = time.time()
    results = grequests.map((grequests.get(u) for u in urls), exception_handler=exception, size=5)
    print(results)
    print("--- %s seconds ---" % (time.time() - start_time))
    return results
