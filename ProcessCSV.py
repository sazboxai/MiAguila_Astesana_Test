import pandas as pd
import os


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


def modify_index(path, file_name, new_index):
    full_dir = path + file_name + '.txt'
    try:
        with open(full_dir, 'w') as file:
            file.writelines(f"processed lines:{new_index}")
        print("The file: " + path + file_name + '.txt' + " has been updated")
    except:
        raise ValueError("The file does not exist or index file has an error or not exist")


def read_data(path, file_name):
    try:
        full_dir = path + file_name
        df = pd.read_csv(full_dir, index_col=None, header=0)
        print(f"the file {path + file_name} has been loaded ")
        return df
    except:
        raise ValueError("Some problem with the local CSV file occur")


def save_invalid_values(df, path, file_name):
    df.to_csv(path + 'invalids' + file_name, mode='a', header=not os.path.exists(path + 'invalids' + file_name),
              index=False)
    print("The file: " + path + 'invalids' + file_name + " has been updated")


def valid_coordinate(lon, lat):
    try:
        if float(lon) != 0.0 and float(lat) != 0.0:
            return True
        else:
            return False
    except:
        return False


def invalids_coordinates(df):
    invalids = []  # ['lon', 'lat', 'problem']
    valid = []  # ['lon', 'lat']

    for index, row in df.iterrows():
        if not valid_coordinate(row['lon'], row['lat']):
            invalids.append([row['lon'], row['lat'], "The coordinates are invalids"])
        else:
            valid.append([row['lon'], row['lat']])
    return pd.DataFrame(valid, columns=['lon', 'lat']), pd.DataFrame(invalids, columns=['lon', 'lat', 'problem'])


def convert_seconds(seconds):
    h = seconds // (60 * 60)
    m = (seconds - h * 60 * 60) // 60
    s = seconds - (h * 60 * 60) - (m * 60)
    return [int(h), int(m), int(s)]
