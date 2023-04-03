def process_last_column(df):
    shortened_labels = ['PartOfAHorizontalPortScan','Okiru','Benign','DDoS','C&C','Attack','C&C-HeartBeat','C&C-FileDownload','C&C-Torii','C&C-HeartBeat-FileDownload','FileDownload','C&C-Mirai','Okiru-Attack']
    for short_label in shortened_labels:
        df.loc[df["label"].str.contains(short_label.casefold(), na=False, case=False), "label"] = short_label
    return df

def data_cleanup():
    # All files  in the path
    import pathlib
    iot_23 = pathlib.Path("/home/mohit/opt/")
    files = list(iot_23.rglob("*.labeled"))
    files = [str(file) for file in files]
    print("\n\n" + str(files) + "\n\n")
    
    import pandas as pd
    consolidated_minified_df = pd.DataFrame()
    initial_skip_rows = 8
    count = 0
    for file_name in files:
        print("-------------xxxxxxxx--------------")
        print("started processing file: " + str(file_name))
        # Loading the dataset
        df = pd.read_table(filepath_or_buffer=file_name, skiprows=initial_skip_rows, low_memory=False)

        df.columns=['ts','uid','id.orig_h','id.orig_p','id.resp_h','id.resp_p','proto','service','duration','orig_bytes','resp_bytes','conn_state','local_orig','local_resp','missed_bytes','history','orig_pkts','orig_ip_bytes','resp_pkts','resp_ip_bytes','label']

        # Remove last row
        df=df.drop(df.index[-1])

        df = df.drop(columns=["uid","id.orig_h","local_orig","local_resp","missed_bytes"])

        df['duration'] = df['duration'].str.replace('-','0')
        df['orig_bytes'] = df['orig_bytes'].str.replace('-','0')
        df['resp_bytes'] = df['resp_bytes'].str.replace('-','0')

        df.fillna(-1,inplace=True)

        df = process_last_column(df)
        
        total_row_count = len(df.index)
        benign_row_count = df['label'].value_counts()['Benign']
        malicious_row_count = total_row_count - benign_row_count

        print("--------------- ORIGINAL FILE -----------------")
        print("total_row_count:" + str(total_row_count))
        print("benign_row_count:" + str(benign_row_count)+  " and ratio: " + str(benign_row_count/total_row_count))
        print("malicious_row_count:" + str(malicious_row_count) +  " and ratio: " + str(malicious_row_count/total_row_count))

        n_rows = int(len(df) * 0.05)
        final_df = pd.DataFrame(columns=df.columns)

        print("--------------- UNDER PROCESS -----------------")
        for label in df['label'].unique(): 
            # there will be 2 labels in each file Benign and malicious label of the file
            label_specific_df = df[df['label'] == label]

            row_count = int(len(label_specific_df) / total_row_count * n_rows)
            print("row count for label: " + label + " is " + str(len(label_specific_df))  + " -> " + str(row_count))
            label_specific_sample = label_specific_df.sample(n=row_count)
            final_df = pd.concat([final_df, label_specific_sample])     

        print("--------------- MINIFIED INDIVIDUAL FILE -------------------------")
        total_row_count = len(final_df.index)
        benign_row_count = final_df['label'].value_counts()['Benign']
        malicious_row_count = total_row_count - benign_row_count
        print("total_row_count:" + str(total_row_count))
        print("benign_row_count:" + str(benign_row_count) +  " and ratio: " + str(benign_row_count/total_row_count))
        print("malicious_row_count:" + str(malicious_row_count) +  " and ratio: " + str(malicious_row_count/total_row_count))
        print("-------------xxxxxxxx--------------")

        final_df.to_csv('minified_datasets/dataset' + str(count) + '.csv', mode='w', index=False, header=True)
        count += 1

if __name__ == '__main__':
    data_cleanup()