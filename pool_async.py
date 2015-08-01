import csv
from multiprocessing import Pool
import os

result_file = "results_"

def search_addresses(list_of_addresses):
    try:
        #output_list = []

        # Do whatever processing needs to be done in parallel
        # Each process writes to its own results file appended by PID
        
        keys = ('add','pin','result')
        print "process: ", str(os.getpid())
        f = open(result_file + str(os.getpid()) + ".csv", 'ab')
        dict_writer = csv.DictWriter(f, keys)
        dict_writer.writeheader()


        """
        l = []
        x = address_dict
        y = results

        s = {}
        s = {'add':x['add'],'pin':x['pin'],'result':y}

        l.append(s)
        dict_writer.writerows(l)
        """

        f.close()

    except Exception as e:
        print e
        pass


if __name__ == "__main__":
    p = Pool(processes = 16)
    f = open('data6.csv')
    reader = csv.reader(f)
    records = []
    #cnt = 100
    keys = reader.next()
    for row in reader:
        if row[1]:
            records.append({"add":row[0],"pin":row[3]})
        """
        cnt += 1
        if cnt > 10000:
            break
        """
    start = 0 
    end = len(records)
    diff = end / 16
    try:
        while start < end:
            p.apply_async(search_addresses, args=(records[start:start+diff], ))
            start = start + diff
        p.close()
        p.join()
    except (KeyboardInterrupt, SystemExit):
        print "Exiting..."
        p.terminate()
        p.join()

