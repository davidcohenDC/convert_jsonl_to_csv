# JSONL to CSV Converter

JSONL to CSV Converter is a high-performance, multi-process tool designed to convert large JSONL files to CSV format seamlessly and efficiently. This tool is extremely useful for data analysts and engineers who need to manipulate large datasets stored in JSONL format and prefer working with CSV for its simplicity and ease of use in data analysis tools, databases, and spreadsheets.

##  Features

- **Multi-Processing:** Utilizes all CPU cores for faster conversion of large files.
- **Robust:** Gracefully handles decoding errors and unexpected issues during conversion, skipping problematic lines while informing the user through logs.
- **Easy to Use:** Simple command line interface requiring input and output file paths.

##  How to Use

### Prerequisites
- Python installed on your system.

### Installation
1. Clone this repository or download the script.
2. Open terminal and navigate to the script's directory.

### Usage
Run the script through the command line using Python, providing it with the input JSONL file path and the desired output CSV file path.

```shell
python script_name.py --input_path your/input/file.jsonl --final_output your/output/file.csv
```

### Example
```shell
python jsonl_to_csv_converter.py --input_path ./home/myfile.jsonl --final_output ./myoutput.csv
```

### Arguments 
* `--input_path`  (Required): The path to the input JSONL file.
* `--final_output` (Required): The path to the final output CSV file.

## Detailed Steps

The script works in the following steps:

1. **Writing Headers**: Extracts headers from the JSONL file and writes them to the CSV.
2. **Processing Lines**: Divides the work among different processes, with each handling a portion of lines from the JSONL file, decoding the JSON and writing it to temporary CSV segments.
3. **Combining CSV Segments**: Merges all CSV segments into the final output CSV file.

### Logging

* The script logs progress, providing details about the number of lines processed, lines skipped due to errors, and the start and completion of each process.
* Logs are printed to the console, keeping the user informed of the script's progress and any issues encountered.

## Contributions
Contributions, issues, and feature requests are welcome!