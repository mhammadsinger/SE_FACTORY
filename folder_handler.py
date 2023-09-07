import os
def find_csv(path):
        csv_list=[]
        for root, dirs, files in os.walk(path):
            for filename in files:
                if filename.endswith('.csv'):
                    csv_list.append(filename)

        return csv_list