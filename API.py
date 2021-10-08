import time
import grequests
import re
import pandas as pd


def api_get_batchs(df_batch, queries_per_sec):
    start_time = time.time()
    df_batches = split_df(df_batch, queries_per_sec)
    postal_codes = []  # ['lon', 'lat', 'postal_code']
    for mini_batch in df_batches:
        # Here I apply the control over the queries to the API (Only allow x queries per second as maximum)
        time_dif = time.time() - start_time
        if time_dif < 1:
            time.sleep(time_dif)
        start_time = time.time()
        # Performing queries to the API
        urls = get_urls(mini_batch)
        postal_codes = postal_codes + get_postal_codes(async_queries(urls))

    return postal_code_df_constructor(df_batch, postal_codes)


def postal_code_df_constructor(df, postal_codes):
    invalids = []  # ['lon', 'lat', 'problem']
    valid = []  # ['lon', 'lat', 'postal_code']
    i = 0
    for index, row in df.iterrows():
        if postal_codes[i] != 'Those coordinates does not have correspondence with any postal code':
            valid.append([row['lon'], row['lat'], postal_codes[i]])
        else:
            invalids.append([row['lon'], row['lat'], postal_codes[i]])
        i += 1
    return pd.DataFrame(valid, columns=['lon', 'lat', 'postal_code']), pd.DataFrame(invalids,
                                                                                    columns=['lon', 'lat', 'problem'])


def split_df(df_batch, size):
    return [df_batch.loc[i:i + size - 1, :] for i in range(0, len(df_batch), size)]


def get_urls(df_mini_batch):
    urls = []
    for index, row in df_mini_batch.iterrows():
        urls.append(f'http://api.postcodes.io/postcodes?lon={row["lon"]}&lat={row["lat"]}')

    return urls


def exception(request, exception):
    print("Problem: {}: {}".format(request.url, exception))


def async_queries(urls):
    start_time = time.time()
    responses = grequests.map((grequests.get(u) for u in urls), exception_handler=exception, size=5)
    return responses


def get_postal_codes(api_responses):
    postal_codes = []
    for elem in api_responses:
        try:
            postal_codes.append(re.search('"postcode":\"(.*?)\",', elem.text).group(1))
        except:
            postal_codes.append('Those coordinates does not have correspondence with any postal code')
    return postal_codes
