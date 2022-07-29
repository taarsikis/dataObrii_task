from datetime import timedelta
import time
import pandas as pd
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import (ClusterOptions, ClusterTimeoutOptions,
                               QueryOptions)
import couchbase.subdocument as SD
# Update this to your cluster
endpoint = "cb.uekiyofl7verp-dc.cloud.couchbase.com"
username = "username"
password = "password"
bucket_name = "travel-sample"

# User Input ends here.
print("Connecting...")
# Connect options - authentication
auth = PasswordAuthenticator(username, password)

# Connect options - global timeout opts
timeout_opts = ClusterTimeoutOptions(kv_timeout=timedelta(seconds=100))

# get a reference to our cluster
cluster = Cluster('couchbases://{}'.format(endpoint),
                  ClusterOptions(auth, timeout_options=timeout_opts))

# Wait until the cluster is ready for use.
cluster.wait_until_ready(timedelta(seconds=5))

# get a reference to our bucket
cb = cluster.bucket(bucket_name)
print("Connected successfully")
print('\n' * 3)

def get_data(bucket_name):
    """
    Connects to every collection and reads data from its documents.
    Compares all data into one pandas DataFrame, where are added 3 columns to understand
     from what scope and collection they were taken.
    Saves results into csv file.

    :param bucket_name:
    :return:
    """

    all_data = []
    print('Downloading data...')
    for scope in cb.collections().get_all_scopes():
        print(scope.name)
        for collection in scope.collections:
            print("     "+collection.name)

            result = cluster.query(f"SELECT *,meta({collection.name}).id as xid FROM `travel-sample`.{scope.name}.{collection.name}",
                                   QueryOptions(metrics=True))
            time.sleep(7)   # have to wait due to server limitations

            rows = [x for x in result.rows()]
            if len(rows):
                df = pd.DataFrame(columns=['scope','collection','xid']+list(rows[0][collection.name].keys()))
                df = df.append([x[collection.name] for x in rows])
                df['xid'] = [x['xid'] for x in rows]
                df['scope'] = scope.name
                df['collection'] = collection.name
                all_data.append(df)
                print('          COLLECTED')
            else:
                print('          EMPTY')

    print("Downloaded successfully")
    print('\n'*3)

    print("Saving to the csv...")

    res = pd.concat(all_data,ignore_index=True,sort=False)
    res.to_csv(f'{bucket_name}.csv')
    print("FINISHED")

def add_new_col():
    """
    Adds a new column to every document (row) in bucket.
    Name of column - 'testColumn', value - 'test'

    :return:
    """
    print('Downloading data...')
    for scope in cb.collections().get_all_scopes():
        print(scope.name)
        for collection in scope.collections:
            print("     "+collection.name)

            cur_collection = cb.scope(scope.name).collection(collection.name)

            result = cluster.query(f"select meta({collection.name}).id from `travel-sample`.{scope.name}.{collection.name}",
                                   QueryOptions(metrics=True))
            rows = [x for x in result.rows()]
            for row in rows:
                cur_collection.mutate_in(
                                    row['id'], [SD.upsert("testColumn", "test")])
            print("          TEST COLUMN ADDED")

def update_csv_with_new_col(col_name, bucket_name):
    """
    Reads data from every scope and collection, but only column, which was given.
    Compares collected data , with the data saved in the csv file.

    :param col_name:
    :param bucket_name:
    :return:
    """
    all_data = []
    print('Downloading data...')
    for scope in cb.collections().get_all_scopes():
        print(scope.name)
        for collection in scope.collections:
            print("     " + collection.name)

            result = cluster.query(
                f"SELECT {col_name},meta({collection.name}).id as xid FROM `travel-sample`.{scope.name}.{collection.name}",
                QueryOptions(metrics=True))
            time.sleep(7)  # have to wait due to server limitations

            rows = [x for x in result.rows()]
            if len(rows):
                df = pd.DataFrame(columns=['scope', 'collection', 'xid', col_name])
                df[col_name] = [x[col_name] for x in rows]
                df['xid'] = [x['xid'] for x in rows]
                df['scope'] = scope.name
                df['collection'] = collection.name
                all_data.append(df)
                print('          COLLECTED')
            else:
                print('          EMPTY')

    print("Downloaded successfully")
    print('\n' * 3)

    print("Saving to the csv...")
    res = pd.concat(all_data, ignore_index=True, sort=False)
    try:
        curr_data = pd.read_csv(f'{bucket_name}.csv', low_memory=False)
    except:
        print('You have no data collected before')
        return
    result = curr_data.merge(res, how='left', on='xid')
    result.to_csv(f'{bucket_name}.csv')
    print("FINISHED")

if __name__ == "__main__":
    bucket_name = "travel-sample"
    get_data(bucket_name)
    add_new_col()
    update_csv_with_new_col('testColumn', bucket_name)