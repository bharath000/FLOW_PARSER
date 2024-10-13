import argparse
import csv
import time
from collections import defaultdict
from utils.logger import get_logger
from utils.flow_log_parser import read_protocol_nums, \
        read_lookup_table, parse_flow_log

log = get_logger("flowparser.log")


def map_flow_logs_to_tags(flow_log_file, lookup_table, 
                            tag_count_file, port_protocol_count_file, 
                            corrupted_file, proto_map, batch_size=10000):

    tag_counts = defaultdict(int)
    port_protocol_counts = defaultdict(int)
    with open(flow_log_file, 'r') as infile, \
         open(corrupted_file, 'w') as corruptfile:
    
        corruptfile.write("Corrupted Flow Log Entry, Error\n")

        corrupt_batch = []

        for line in infile:
            result, error = parse_flow_log(line, proto_map)

            if result:
                dstport, protocol = result
                # import pdb
                # pdb.set_trace()
                tag = lookup_table.get((dstport, protocol), "Untagged")
                tag_counts[tag] += 1
                port_protocol_counts[(dstport, protocol)] += 1
            else:
                corrupt_batch.append(f"{line.strip()}, {error}\n")

            if len(corrupt_batch) >= batch_size:
                corruptfile.writelines(corrupt_batch)
                corrupt_batch.clear()

        if corrupt_batch:
            corruptfile.writelines(corrupt_batch)
    
    with open(tag_count_file, 'w') as tagcountfile:
        tagcountfile.write("Tag,Count\n")
        for tag, count in tag_counts.items():
            tagcountfile.write(f"{tag},{count}\n")

    with open(port_protocol_count_file, 'w') as portprotocolfile:
        portprotocolfile.write("Port,Protocol,Count\n")
        for (port, protocol), count in port_protocol_counts.items():
            portprotocolfile.write(f"{port},{protocol},{count}\n")


def main():
    start_time = time.time()
    parser = argparse.ArgumentParser(description="parse the flow logs data")
    parser.add_argument('log_file', help="file path for log file")
    parser.add_argument('lookup_file', help="file path for look up data")
    parser.add_argument('tag_count_file', help="file path for tag count output")
    parser.add_argument('port_protocol_count_file', help="file path for port protocol output file")
    parser.add_argument('corrupted_file', help="file path for corrupted flow logs")

    args = parser.parse_args()
    log.info("args provided for the rum {}".format(args))
    proto_map = read_protocol_nums("protocol-numbers-1.csv")

    lookup_table = read_lookup_table(args.lookup_file, proto_map)

    map_flow_logs_to_tags(args.log_file, lookup_table,
                          args.tag_count_file,
                          args.port_protocol_count_file,
                          args.corrupted_file,
                          proto_map)

    end_time = time.time()
    log.info("execution time: {}".format(end_time - start_time))
    


if __name__ == "__main__":
    main()
