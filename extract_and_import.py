import subprocess
import pandas as pd
from sqlalchemy import create_engine
import datetime
engine = create_engine('postgresql://postgres:mysecretpassword@localhost:5432/mylocaldb')


def return_udr_file_list():
    """
    List of UDR files
    :return:
    """
    proc = subprocess.run(["ls"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    dec_proc = proc.stdout.decode()
    item_list = dec_proc.split(sep="\n")
    return [x for x in item_list if '.udr.' in x]


def return_udr_files_extracted():
    """
    List of extracted items
    :return list comprehension:
    """
    proc = subprocess.run(["ls"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    dec_proc = proc.stdout.decode()
    item_list = dec_proc.split(sep="\n")
    return [x for x in item_list if '.udr.' not in x and '.udr' in x]


def subcall(item):
    '''
    Run our tar extract linux command function
    :param item:
    :return extract stdout:
    '''
    extract = subprocess.run(["tar", "-zxvf", item], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, timeout=20)
    # print(extract.stderr)
    # print(extract.stdout)

    return extract.stdout


def extract_items(data_list):
    """
    Run our extract items function
    :param data_list:
    :return:
    """
    print(data_list)
    for item in data_list:
        print(item)
        subcall(item)


def ReadAllUDR():
    """
    # Logic : Create a new file
    # open each UDR file extracted and read each line
    # Write each line to new file.
    # Close both files
    """
    with open('newUDRfile.udr', mode='a') as new_open_file:
        for file in return_udr_files_extracted():
            with open(file) as open_file:
                for line in open_file.readlines():
                    new_open_file.write(line)
            open_file.close()
    new_open_file.close()


def CreateDataFrames():

    """
    1) Create dataframe 1 (containing all UDRS
    2) Dataframe 2 (subscribers)
    3) Dataframe 3 (sessions)
    4) Save to Database
    :return None:
    """
    df = pd.read_csv('newUDRfile.udr')

    UDR_headers = ['RecordType', 'RecordStatus', 'RecordNumber', 'StartTime', 'EndTime', 'AcctSessionId',
                   'SubscriberId', 'FramedIp', 'ServiceId', 'TotalBytes', 'RxBytes', 'TxBytes', 'Time']

    # all UDR header columns
    df.columns = UDR_headers
    df["SubscriberId"] = df["SubscriberId"].str.strip()
    df["AcctSessionId"] = df["AcctSessionId"].str.strip()
    df["StartTime"] = df["StartTime"].str.strip()
    df["EndTime"] = df["EndTime"].str.strip()

    # Create Subscriber DATAFRAME
    # table format : subscriber id | subscriber name
    subscriber_list = [x for x in df["SubscriberId"].unique()]
    df2 = pd.DataFrame(data=subscriber_list, columns=['subscriber_name'])
    df2.insert(loc=0, column='subscriber_id', value=range(0, len(df2['subscriber_name'])))

    # Create Session DATAFRAME
    # Session name list
    # table format : session id | session name | total | rx | tx | start | end
    session_name_list = [x for x in df["AcctSessionId"].unique()]

    large_list = []

    for item in session_name_list:
        temp_dict = {}
        temp_dict['session_id'] = str(item)
        temp_dict['subscriber_name'] = df[df["AcctSessionId"] == item].SubscriberId.unique()[0]
        temp_dict['start_date'] = df[df["AcctSessionId"] == item].StartTime.iloc[0]
        temp_dict['totalbytes'] = df[df["AcctSessionId"] == item].TotalBytes.sum()
        temp_dict['rx_bytes'] = df[df["AcctSessionId"] == item].RxBytes.sum()
        temp_dict['tx_bytes'] = df[df["AcctSessionId"] == item].TxBytes.sum()
        large_list.append(temp_dict)

    # Save to DataFrame
    df3 = pd.DataFrame(large_list)

    # Save to Database
    date = datetime.date.today().strftime("%d_%m_%y")

    try:
        df.to_sql(name='udrtable_' + date, con=engine, )
        df2.to_sql(name='subscribertable_' + date, con=engine)
        df3.to_sql(name='sessiontable_' + date, con=engine)
    except:
        print('An error has occurred')
    else:
        print('Saved the following to Database successfully: \n{a}\n{b}\n{c}'.format(a='udrtable_' + date, b='subscribertable_' + date, c='sessiontable_' + date))


def main():
    """
    1 - Extract all zipped UDR files
    2 - Read all UDR's into one file
    3 - import UDR file and create Dataframes
    4 - Save to Database
    :return:
    """

    extract_items(data_list=return_udr_file_list())

    try:
        ReadAllUDR()
    except:
        print('file not found, exiting')


    CreateDataFrames()

if __name__ == '__main__':
    main()




