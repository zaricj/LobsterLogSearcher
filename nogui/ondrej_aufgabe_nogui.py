import cProfile
import re
import csv
from tqdm import tqdm
import os

# Pattern A: Regular job line with optional parent
PATTERN_JOB = re.compile(
    r"(?P<time>\d{2}:\d{2}:\d{2})\s+.*?Job:\s+(?P<job_id>\d+)\s+\[(?P<profile>[^\]]+)]\s+(?P<system>[^:]+):.*?(?:Parent job ID is (?P<parent_id>\d+))?"
)

# Pattern B: Child process line
PATTERN_CHILD = re.compile(
    r"(?P<time>\d{2}:\d{2}:\d{2})\s+.*?Job:\s+(?P<job_id>\d+)\s+\[(?P<profile>[^\]]+)]\s+(?P<system>[^:]+):Child process '(?P<child_profile>[^']+)' has started; new job is (?P<new_job>\d+)"
)

def extract_info_from_line(line, date):
    match_job = PATTERN_JOB.search(line)
    match_child = PATTERN_CHILD.search(line)

    if match_child:
        # Match for child process line
        return [
            f"{date} {match_child.group('time')}",
            match_child.group('job_id'),
            "None",
            match_child.group('profile'),
            match_child.group('system'),
            f"Child process '{match_child.group('child_profile')}' has started",
            match_child.group('new_job')
        ]

    elif match_job:
        # Match for regular job line
        return [
            f"{date} {match_job.group('time')}",
            match_job.group('job_id'),
            match_job.group('parent_id') if match_job.group('parent_id') else "None",
            match_job.group('profile'),
            match_job.group('system'),
            "None",
            "None"
        ]

    return None


def process_file(filepath):
    """Processes a single log file.

    Args:
        filepath: Path to the log file.

    Returns:
        A list containing the extracted information from the log file.
    """
    matching_lines = []
    filename = os.path.basename(filepath)[:10] # If filename is like 13_05_2025_message.log then 13_05_2025 will be outputted only

    with open(filepath, "r", encoding="utf-8") as log_file:
        total_lines = sum(1 for _ in log_file)  # Get total lines in the file
        log_file.seek(0)  # Reset file pointer to start

        # Initialize tqdm progress bar for each line
        progress_bar = tqdm(total=total_lines, desc=f"Processing {os.path.basename(filepath)}", unit=" lines")

        for line in log_file:
            # Extract information from the line
            extracted_info = extract_info_from_line(line, filename)
            if extracted_info:
                matching_lines.append(extracted_info)
            progress_bar.update(1)  # Update progress bar for each line processed

        progress_bar.close()

    return matching_lines


def extract_and_write_to_csv(filepath, output_file_csv="extracted_info.csv"):
    """Extracts job numbers, profile names, filenames, and filesizes from log files and writes them to a CSV.

    Args: 
        filepath: The path to the log file or directory containing log files.
        output_file_csv (optional): The name of the output CSV file (defaults to "extracted_info.csv").
    """
    
    if not os.path.exists(output_file_csv):
        os.makedirs(os.path.dirname(output_file_csv), exist_ok=True)
        print("Directory did not exist, created it. ")

    print("Starting app, please wait...")
    
    if os.path.isfile(filepath):
        files = [filepath]
    elif os.path.isdir(filepath):
        files = [os.path.join(filepath, f) for f in os.listdir(filepath) if f.endswith("_message.log")]
    else:
        print("Invalid filepath.")
        return

    data = []

    for file in tqdm(files, desc="Processing Files"):
        data.extend(process_file(file))
    
    try:
        with open(output_file_csv, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["Time", "Job ID", "Parent Job ID", "Profile Name", "System Name", "System Run", "New Job"]) # Header row
            writer.writerows(data)
            
        print(f"Data has been written to {output_file_csv}")
        print(f"Total Matches: {len(data)}")
    except Exception as e:
        print("Error writing to CSV:", e)

if __name__ == "__main__":
    # Profile the main function
    #profiler = cProfile.Profile()
    #profiler.enable()

    # Run main function, change log file path accordingly
    filepath = r"C:\Users\ZaricJ\Downloads\NESISNCT01 Logs\Logs"
    output_csv = r"C:\Users\ZaricJ\Downloads\NESISNCT01 Logs\CSV\results_new.csv"

    extract_and_write_to_csv(filepath, output_csv)

    #profiler.disable()
    #profiler.print_stats()
