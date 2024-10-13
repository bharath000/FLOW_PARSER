import argparse
import csv
import time
import queue
import threading
from collections import defaultdict
from utils.logger import get_logger
from utils.flow_log_parser import read_protocol_nums, \
        read_lookup_table, parse_flow_log


log = get_logger("flowparser.log")

def map_flow_logs_to_tags(q, lookup_table, tag_counts, port_protocol_counts, proto_map, lock):

    while True:
        line = q.get()
        if line is None:
            break  # Exit condition
        result, error = parse_flow_log(line, proto_map)
        if result:
            dstport, protocol = result
            tag = lookup_table.get((dstport, protocol), "Untagged")
            with lock:
                tag_counts[tag] += 1
                port_protocol_counts[(dstport, protocol)] += 1
        else:
            log.info(f"{line.strip()}, {error}\n")

        q.task_done()


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

    q = queue.Queue(maxsize=10000)
    tag_counts = defaultdict(int)
    port_protocol_counts = defaultdict(int)
    lock = threading.Lock()

    num_threads = 5

    threads = []
    for _ in range(num_threads):
        t = threading.Thread(target=map_flow_logs_to_tags, args=(q, lookup_table, tag_counts, port_protocol_counts, proto_map, lock))
        t.start()
        threads.append(t)

    # Step 4: Enqueue flow log entries
    with open(args.log_file, 'r') as infile:
        for line in infile:
            q.put(line.strip())
    
    # Step 5: Signal threads to exit
    for _ in range(num_threads):
        q.put(None)

    # Step 6: Wait for all threads to finish
    for t in threads:
        t.join()

    with open(args.tag_count_file, 'w') as tagcountfile:
        tagcountfile.write("Tag,Count\n")
        for tag, count in tag_counts.items():
            tagcountfile.write(f"{tag},{count}\n")

    with open(args.port_protocol_count_file, 'w') as portprotocolfile:
        portprotocolfile.write("Port,Protocol,Count\n")
        for (port, protocol), count in port_protocol_counts.items():
            portprotocolfile.write(f"{port},{protocol},{count}\n")

    end_time = time.time()
    log.info("execution time: {}".format(end_time - start_time))
    


if __name__ == "__main__":
    main()
