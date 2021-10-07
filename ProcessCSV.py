import pandas as pd


def read_index(path, file_name):
    full_dir = path + file_name + '.txt'
    try:
        f = open(full_dir)
        str_line = f.readline().rstrip()
        index = int(str_line.split(':')[1])
        f.close()
        return index
    except:
        raise ValueError("The index file has an error or not exist")


def read_data(path, file_name):
    try:
        full_dir = path + file_name
        df = pd.read_csv(full_dir, index_col=None, header=0)
        print(f"the file {path + file_name} has been loaded ")
        return df
    except:
        raise ValueError("Some problem with the local CSV file occur")


def valid_coordinate(lon, lat):
    try:
        if float(lon) != 0.0 and float(lat) != 0.0:
            return True
        else:
            return False
    except:
        return False
