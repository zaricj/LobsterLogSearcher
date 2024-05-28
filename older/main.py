import re
import csv
from tqdm import tqdm

def extract_info_from_log(path_to_log_file):
    """Extracts time, job numbers, profile names, filenames, and filesizes from a log file.

    Args:
        path_to_log_file: The folder path to the log file.

    Returns:
        A list containing the extracted information.
    """
    matching_lines = []
    global total_matches
    total_matches = 0

    with open(path_to_log_file, "r", encoding="utf-8") as log_file:
        for line in tqdm(log_file, desc="Iteration Progress"):
            # Patterns for extracting information
            time_pattern = r"\b(\d{2}:\d{2}:\d{2})\b"
            job_number_pattern = r"Job:\s+((?:\d+|GENERAL))"
            profilename_pattern = r"\[(.*?)\)]"
            filename_pattern = r"Start processing data of file '(.*?)'"
            filesize_pattern = r"length=(\d+),"
            log_filename_pattern = r"\d{4}_\d{2}_\d{2}_message\.log" # Only files that are called yyyy_mm_dd_message.log

            # Extract information using regular expressions
            time_match = re.search(time_pattern, line)
            job_number_match = re.search(job_number_pattern, line)
            profilename_match = re.search(profilename_pattern, line)
            filename_match = re.search(filename_pattern, line)
            filesize_match = re.search(filesize_pattern, line)

            if time_match and profilename_match and filename_match and filesize_match and job_number_match:
                time = time_match.group(1)
                job_numbers = job_number_match.group(1)
                profilenames = profilename_match.group(1)
                filenames = filename_match.group(1)
                filesizes = filesize_match.group(1)
                matching_lines.append([time, job_numbers, profilenames, filenames, filesizes])

        total_matches = len(matching_lines)

    return matching_lines


def extract_and_write_to_csv(filepath, output_file_csv="extracted_info.csv"):
    """Extracts job numbers, profile names, filenames, and filesizes from a log file and writes them to a CSV.

    Args: 
        filepath: The path to the log file.
        output_file_csv (optional): The name of the output CSV file (defaults to "extracted_info.csv").
    """

    # Extracted information using regular expressions as list
    data = extract_info_from_log(filepath)

    try:
        with open(output_file_csv, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["Time", "Job Number", "Profile Name", "Filename", "Filesize in Bytes"])  # Header row
            writer.writerows(data)
        print(f"Data has been written to {output_file_csv}")
        print(f"Total Matches: {total_matches}")
    except Exception as e:
        print("Error writing to CSV:", e)

if __name__ == "__main__":
    
    # Run main function, change log file path accordingly
    filepath = "//nesis002/hub/logs/DataWizard/2024_04_22_message.log"
    output_csv = "results.csv"

    extract_and_write_to_csv(filepath, output_csv)
