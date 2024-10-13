import csv
import time
from collections import defaultdict
from utils.logger import get_logger


log = get_logger("flowparser.log")

def read_protocol_nums(filename):
    """
    description:
        the file should in csv format, read the csv file
        and gives the portocol to number map
    parameters:
        filename: name of the that in repository
    return:
        dictionary/map with number->protocols as key->value
    """
    #TODO dynaically update protocols file and support multiple file formats
    if(filename.split('.')[-1]!="csv"):
        log.error("error file need to be csv")
    protocol_map = {}
    with open(filename, mode='r') as protocol_file:
        reader = csv.reader(protocol_file)
        header = next(reader)
        for row in reader:
            if '-' in row[0]:
                x, y = row[0].split("-")
                log.info("protocol data has range based nummbers")
                for num in range(int(x), int(y)+1):
                    protocol_map[num]=row[1].lower()
            else:
                protocol_map[row[0]]=row[1].lower()
    log.info("created protocol map: \n{}".format(protocol_map))
    return protocol_map

def read_lookup_table(lookup_file, proto_map):
    lookup_table = {}
    with open(lookup_file, 'r') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # Skip the header
        
        for row_num, row in enumerate(reader, start=1):
            try:
                if len(row) != 3:
                    print(f"Skipping malformed row {row_num}: {row}")
                    continue
                
                dstport = row[0].strip()
                protocol = row[1].strip().lower()
                tag = row[2].strip()
                
                lookup_table[(dstport, protocol)] = tag
            
            except Exception as e:
                print(f"Error processing row {row_num}: {e}")
                continue  # Skip any rows that raise exceptions
    log.info("lookup table info {}".format(lookup_table))
    return lookup_table


def parse_flow_log(log_line, proto_map):
    fields = log_line.split()
    # import pdb
    # pdb.set_trace()
    if len(fields) != 14:
        return None, "Invalid number of fields"
    
    try:
        dstport = fields[6]
        srcport = int(fields[5])
        if not (1 <= int(dstport) <= 65535 and 1 <= srcport <= 65535):
            return None, "Invalid port number"
        
        protocol_num = fields[7]
        protocol = proto_map[protocol_num]

        if not protocol:
            return None, "Invalid protocol"
        
        action = fields[12]
        if action not in ["ACCEPT", "REJECT"]:
            return None, "Invalid action"
        
        log_status = fields[13]
        if log_status not in ["OK", "NODATA", "SKIPPED"]:
            return None, "Invalid log status"
        
        return (dstport, protocol), None

    except (ValueError, IndexError):
        return None, "Parsing error"

