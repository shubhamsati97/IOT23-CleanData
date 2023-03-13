# Reference:
# https://github.com/yliang725/Anomaly-Detection-IoT23/blob/main/Data%20Preprocessing/IoT23%20-%20Data%20Preprocessing.ipynb


import pathlib
import pandas as pd

iot_23 = pathlib.Path("iot_23")
files = list(iot_23.rglob("*.labeled"))
files = [str(file) for file in files]

dfs = []

for file in files:
    df = pd.read_table(filepath_or_buffer=file, skiprows=8)
    df.columns = ['ts',
                  'uid',
                  'id.orig_h',
                  'id.orig_p',
                  'id.resp_h',
                  'id.resp_p',
                  'proto',
                  'service',
                  'duration',
                  'orig_bytes',
                  'resp_bytes',
                  'conn_state',
                  'local_orig',
                  'local_resp',
                  'missed_bytes',
                  'history',
                  'orig_pkts',
                  'orig_ip_bytes',
                  'resp_pkts',
                  'resp_ip_bytes',
                  'label']
    df.drop(df.tail(1).index, inplace=True)
    dfs.append(df)

df_main = pd.concat(dfs)

# Clean df labels

df_main.loc[(df_main.label == '-   Malicious   PartOfAHorizontalPortScan'), 'label'] = 'PartOfAHorizontalPortScan'
df_main.loc[(df_main.label == '(empty)   Malicious   PartOfAHorizontalPortScan'), 'label'] = 'PartOfAHorizontalPortScan'
df_main.loc[(df_main.label == '-   Malicious   Okiru'), 'label'] = 'Okiru'
df_main.loc[(df_main.label == '(empty)   Malicious   Okiru'), 'label'] = 'Okiru'
df_main.loc[(df_main.label == '-   Benign   -'), 'label'] = 'Benign'
df_main.loc[(df_main.label == '(empty)   Benign   -'), 'label'] = 'Benign'
df_main.loc[(df_main.label == '-   Malicious   DDoS'), 'label'] = 'DDoS'
df_main.loc[(df_main.label == '-   Malicious   C&C'), 'label'] = 'C&C'
df_main.loc[(df_main.label == '(empty)   Malicious   C&C'), 'label'] = 'C&C'
df_main.loc[(df_main.label == '-   Malicious   Attack'), 'label'] = 'Attack'
df_main.loc[(df_main.label == '(empty)   Malicious   Attack'), 'label'] = 'Attack'
df_main.loc[(df_main.label == '-   Malicious   C&C-HeartBeat'), 'label'] = 'C&C-HeartBeat'
df_main.loc[(df_main.label == '(empty)   Malicious   C&C-HeartBeat'), 'label'] = 'C&C-HeartBeat'
df_main.loc[(df_main.label == '-   Malicious   C&C-FileDownload'), 'label'] = 'C&C-FileDownload'
df_main.loc[(df_main.label == '-   Malicious   C&C-Torii'), 'label'] = 'C&C-Torii'
df_main.loc[(df_main.label == '-   Malicious   C&C-HeartBeat-FileDownload'), 'label'] = 'C&C-HeartBeat-FileDownload'
df_main.loc[(df_main.label == '-   Malicious   FileDownload'), 'label'] = 'FileDownload'
df_main.loc[(df_main.label == '-   Malicious   C&C-Mirai'), 'label'] = 'C&C-Mirai'
df_main.loc[(df_main.label == '-   Malicious   Okiru-Attack'), 'label'] = 'Okiru-Attack'

# Drop columns
df_main = df_main.drop(columns=['ts', 'uid', 'id.orig_h', 'id.orig_p', 'id.resp_h', 'id.resp_p',
                                'service', 'local_orig', 'local_resp', 'history'])

# Replace '-' with 0
df_main['duration'] = df_main['duration'].str.replace('-', '0')
df_main['orig_bytes'] = df_main['orig_bytes'].str.replace('-', '0')
df_main['resp_bytes'] = df_main['resp_bytes'].str.replace('-', '0')

df_main.to_csv('iot23_new.csv')
