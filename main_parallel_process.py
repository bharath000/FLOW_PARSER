import argparse
import csv
import multiprocessing
import time
from utils.flow_log_parser import read_protocol_nums, read_lookup_table, parse_flow_log


def map_flow_logs_to_tags(chunk, lookup_table, proto_map):
    """
    Process a chunk of flow logs and map them to tags based on the lookup table.
    """
    tag_counts = {}
    port_protocol_counts = {}

    for line in chunk:
        result, error = parse_flow_log(line, proto_map)
        if result:
            dstport, protocol = result
            tag = lookup_table.get((dstport, protocol), "Untagged")
            tag_counts[tag] = tag_counts.get(tag, 0) + 1
            port_protocol_counts[(dstport, protocol)] = port_protocol_counts.get((dstport, protocol), 0) + 1

    return tag_counts, port_protocol_counts


def aggregate_results(results):
    """
    Aggregates the results from all worker processes into a single summary.
    """
    final_tag_counts = {}
    final_port_protocol_counts = {}

    for tag_counts, port_protocol_counts in results:
        for tag, count in tag_counts.items():
            final_tag_counts[tag] = final_tag_counts.get(tag, 0) + count
        for (port, protocol), count in port_protocol_counts.items():
            final_port_protocol_counts[(port, protocol)] = final_port_protocol_counts.get((port, protocol), 0) + count

    return final_tag_counts, final_port_protocol_counts


def main():
    start_time = time.time()
    parser = argparse.ArgumentParser(description="Flow log processing with multiprocessing.")

    # Command-line arguments for file paths
    parser.add_argument('flow_log_file', help="Path to the flow log file")
    parser.add_argument('lookup_file', help="Path to the lookup table CSV file")
    parser.add_argument('tag_count_file', help="Path to the output file for tag counts")
    parser.add_argument('port_protocol_count_file', help="Path to the output file for port/protocol counts")

    args = parser.parse_args()

    # Read protocol numbers and lookup table in the main process
    proto_map = read_protocol_nums("protocol-numbers-1.csv")
    lookup_table = read_lookup_table(args.lookup_file, proto_map)

    # Read the flow log file
    with open(args.flow_log_file, 'r') as infile:
        flow_logs = infile.readlines()

    # Split flow logs into chunks for each worker
    num_workers = multiprocessing.cpu_count()
    chunk_size = len(flow_logs) // num_workers
    chunks = [flow_logs[i:i + chunk_size] for i in range(0, len(flow_logs), chunk_size)]

    pool = multiprocessing.Pool(processes=num_workers)

    # Process the chunks in parallel and collect results
    results = pool.starmap(map_flow_logs_to_tags, [(chunk, lookup_table, proto_map) for chunk in chunks])

    pool.close()
    pool.join()

    # Aggregate the results
    final_tag_counts, final_port_protocol_counts = aggregate_results(results)

    # Write tag counts to output file
    with open(args.tag_count_file, 'w') as tagcountfile:
        tagcountfile.write("Tag,Count\n")
        for tag, count in final_tag_counts.items():
            tagcountfile.write(f"{tag},{count}\n")

    # Write port/protocol counts to output file
    with open(args.port_protocol_count_file, 'w') as portprotocolfile:
        portprotocolfile.write("Port,Protocol,Count\n")
        for (port, protocol), count in final_port_protocol_counts.items():
            portprotocolfile.write(f"{port},{protocol},{count}\n")

    end_time = time.time()
    print(f"Execution time: {end_time - start_time:.2f} seconds")


if __name__ == "__main__":
    main()
