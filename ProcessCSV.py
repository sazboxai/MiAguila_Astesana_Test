import pandas as pd


def read_data(path, file_name):
    full_dir = path + file_name
    df = pd.read_csv(full_dir, index_col=None, header=0)
    return df


def valid_coordinate(lon, lat):
    try:
        if float(lon) != 0.0 and float(lat) != 0.0:
            return True
        else:
            return False
    except:
        return False


# df = read_data('data/', 'test.csv')
print(valid_coordinate(None, 0))
a = 5
