import json
import csv
import multiprocessing
import os
import argparse


def process_lines(start, end, input_path, output_segment):
    print(f'Process {start}-{end}: Starting process for lines {start} to {end}, writing to {output_segment}')

    processed_lines = 0
    skipped_lines_due_to_decoding = 0
    skipped_lines_due_to_error = 0

    with open(input_path, 'r', encoding='utf-8') as jsonl_file:
        with open(output_segment, 'w', newline='', encoding='utf-8-sig') as csv_file:
            writer = csv.writer(csv_file, escapechar='\\', quoting=csv.QUOTE_NONNUMERIC)

            for current_line, line in enumerate(jsonl_file, start=1):
                if start <= current_line <= end:
                    try:
                        data = json.loads(line)
                        writer.writerow(data.values())
                        processed_lines += 1

                        if current_line % 1000 == 0:
                            print(f'Process {start}-{end}: Processed line {current_line}')
                    except json.JSONDecodeError:
                        print(f'Process {start}-{end}: Could not decode line {current_line}, skipping')
                        skipped_lines_due_to_decoding += 1
                    except Exception as e:
                        print(f'Process {start}-{end}: Unexpected error on line {current_line}, skipping: {e}')
                        skipped_lines_due_to_error += 1

    print(f'Process {start}-{end}: Processed {processed_lines} lines in total')
    print(f'Process {start}-{end}: Skipped {skipped_lines_due_to_decoding} lines due to decoding errors')
    print(f'Process {start}-{end}: Skipped {skipped_lines_due_to_error} lines due to unexpected errors')
    print(f'Process {start}-{end}: Process for lines {start} to {end} completed')


def write_headers(input_path, output_path):
    with open(input_path, 'r', encoding='utf-8') as jsonl_file:
        first_line = jsonl_file.readline()
        headers = json.loads(first_line).keys()

    with open(output_path, 'w', newline='', encoding='utf-8') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(headers)


def combine_csv(final_output, segments):
    print("Starting combining CSV segments into the final output...")
    if not segments:
        print("No segments provided for combining, exiting...")
        return

    with open(final_output, 'a', newline='', encoding='utf-8') as output_file:
        writer = csv.writer(output_file)
        print(f"Writing to {final_output}")

        for segment in segments:
            print(f"Processing segment: {segment}")
            if os.path.exists(segment):

                if os.path.getsize(segment) > 0:
                    try:
                        with open(segment, 'r', encoding='utf-8') as infile:
                            data = infile.read().replace('\0', '')
                        with open(segment, 'w', encoding='utf-8') as outfile:
                            outfile.write(data)

                        with open(segment, 'r', encoding='utf-8') as input_file:
                            reader = csv.reader(input_file)
                            print(f"Combining segment {segment} into {final_output}")
                            for row in reader:
                                writer.writerow(row)
                        os.remove(segment)  # rimuove il segmento dopo l'unione
                        print(f"Segment {segment} processed and removed.")
                    except Exception as e:
                        print(f'Error processing segment {segment}, skipping. Error: {e}')
                else:
                    print(f'Segment {segment} is empty, skipping.')
            else:
                print(f'Segment {segment} does not exist, skipping.')
    print("Combining process completed.")


def main(args):
    input_path = args.input_path
    final_output = args.final_output
    write_headers(input_path, final_output)

    total_lines = sum(1 for _ in open(input_path, 'r', encoding='utf-8'))
    num_processes = multiprocessing.cpu_count()
    lines_per_process = total_lines // num_processes

    processes = []
    segments = []
    for i in range(num_processes):
        start_index = i * lines_per_process + 1
        end_index = (i + 1) * lines_per_process if i != num_processes - 1 else total_lines
        segment_file = f'segment_{i}.csv'
        segments.append(segment_file)
        p = multiprocessing.Process(target=process_lines, args=(start_index, end_index, input_path, segment_file))
        processes.append(p)
        p.start()

    for process in processes:
        process.join()

    combine_csv(final_output, segments)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('input_path', default="'file.jsonl'", help='the path to the input JSONL file')
    parser.add_argument('final_output', default="output.csv", help='the path to the final output CSV file')
    parser_args = parser.parse_args()
    main(parser_args)
