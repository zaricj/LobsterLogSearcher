import os
import re

def count_log_files(folder_path):
    
    log_counter = 0
    pattern = r"\d{4}_\d{2}_\d{2}_message\.log"
    list_matched_logs = []
    
    for filename in os.listdir(folder_path):
        if filename.endswith(".log"):
            if re.match(pattern, filename):
                filepath = os.path.join(folder_path, filename)
                log_counter += 1
                list_matched_logs.append(filename)
    
    matched_logs_str = ", ".join(list_matched_logs)
    print(f"A total of {log_counter} log files with names {matched_logs_str} have been found in folder {folder_path}")

folder_path = "//nesis002/hub/logs/DataWizard"
count_log_files(folder_path)
