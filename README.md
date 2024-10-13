# Flow Log Parser with Multiprocessing

## Overview

This project is a Python-based flow log parser to efficiently process large flow log files. The program reads a log file containing network flow data, applies tags based on a lookup table and a protocol number mapping, and produces summary reports on tag counts and port/protocol combinations.

The logs are processed in parallel across multiple CPU cores for faster execution, and logging is centralized to avoid conflicts between processes.

## Features

- **Sequncialprocessing**: Reads and process logs in a sequntial manner
- **Multithreading**: Reads and process logs using threads in a pub-sub model
- **Multiprocessing**: Efficiently process large datasets by splitting the work across multiple cores.
- **Protocol Mapping**: Support for a protocol number-to-name mapping using a CSV file.
- **Tagging**: Apply tags to flow log entries based on destination port and protocol using a lookup table.
- **Centralized Logging**: Log messages from worker processes are sent to a centralized logging system to avoid duplication or conflicts.
- **Output**: Summary reports include counts of tags and port/protocol combinations.


## Requirements

- Python 3.8 or later

## Usage

## sequncial approach
```
python3 main.py flowlog_text_file lookuptable_text_file tag_counts_file port_protocol_count corrupted_file
python3 main.py data/large_100MB_flow_log.txt data/lookup_table.txt tag_count.csv port_protocol_count.csv corrupted_flows.csv
```
## threading approach
```
python3 main.py flowlog_text_file lookuptable_text_file tag_counts_file port_protocol_count corrupted_file
python3 main_parallel.py data/large_100MB_flow_log.txt data/lookup_table.txt tag_count.csv port_protocol_count.csv corrupted_flows.csv
```
## multiprocess approach
```
python3 main.py flowlog_text_file lookuptable_text_file tag_counts_file port_protocol_count
python3 main_parallel_process.py data/large_100MB_flow_log.txt data/lookup_table.txt tag_count.csv port_protocol_count.csv
```