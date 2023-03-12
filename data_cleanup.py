def process_last_column(data):
    shortened_labels = ['PartOfAHorizontalPortScan','Okiru','Benign','DDoS','C&C','Attack','C&C-HeartBeat','C&C-FileDownload','C&C-Torii','C&C-HeartBeat-FileDownload','FileDownload','C&C-Mirai','Okiru-Attack']
    for short_label in shortened_labels:
        if short_label.casefold() in data[len(data)-1].casefold():
            data[len(data)-1] = short_label
            break
    return data

def convert_hyphens_to_zero(data, fields):
    column_no = 0
    numerical_columns = {'duration','orig_bytes','resp_bytes'}
    try:
        for column in data:
            if column == "-" and fields[column_no] in numerical_columns:
                data[column_no] = 0
            column_no += 1
        return data
    except Exception as e:
        print("-----------xxxx----------\n")
        print("exception: " + str(e))
        print("-----------xxxx----------\n")

def data_cleanup():
    # All files  in the path
    import pathlib
    iot_23 = pathlib.Path("/home/mohit/dataset/data/opt/")
    files = list(iot_23.rglob("*.labeled"))
    files = [str(file) for file in files]
    print(files)


    file_fields = None
    frames = list()
    to_remove = {"ts","uid","id.orig_h","local_orig","local_resp","missed_bytes"}
    column_no_to_remove = list()
    initial_skip_rows = 8
    header_written = False
    for file_name in files[:2]:
        # Get all columns from the file
        with open(file_name,'r') as f:
            lines = f.readlines()
            lines = lines[:10]
            file_fields = lines[6].split('\t')
            file_fields = file_fields[1:]

        final_file_fields = list()
        print(len(file_fields))

        column_no = 0
        for field in file_fields:
            if field in to_remove:
                column_no_to_remove.append(column_no)
            else:
                final_file_fields.append(field)
            column_no += 1
        print("FOUND : " + str(column_no_to_remove))
        csv_file = "consolidated_dataset.csv"

        row_count = 1
        data = None
        data_length = None
        with open(file_name,'r') as file:
            print("starting processing of " + file_name + ":")
            while (line := file.readline()):
                if row_count > 8:
                    import csv
                    try:
                        if not header_written:
                            with open(csv_file, 'a') as csvfile:
                                writer = csv.writer(csvfile, delimiter=',', lineterminator='\n')
                                final_file_fields[len(final_file_fields)-1] = "label"
                                writer.writerow(final_file_fields)
                                header_written = True

                        data = line.split('\t')
                        if len(data) > 1 and not "#close" in  data[0]:
                            # print("before row to be written: " + str(data))
                            data_length = len(data)
                            for column_no in column_no_to_remove:
                                del data[column_no]
                            # print("final row to be written: " + str(data))
                            data = process_last_column(data)
                            data = convert_hyphens_to_zero(data, final_file_fields)
                            with open(csv_file, 'a') as csvfile:
                                writer = csv.writer(csvfile, delimiter=',', lineterminator='\n')
                                writer.writerow(data)
                    except Exception as e:
                        print("EXCEPTION")
                        print("-----------xxxx----------\n" + str(data_length) + "\n" + str(row_count) + "\n" + str(data))
                        print(str(e))
                        print("-----------xxxx----------\n")
                row_count += 1
        column_no_to_remove = list()


if __name__ == '__main__':
    data_cleanup()