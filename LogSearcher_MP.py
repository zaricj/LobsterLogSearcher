import PySimpleGUI as sg
from tqdm import tqdm
from multiprocessing import Pool
import pandas as pd
import csv
import re
import os
import re

PROGRAM_ICON = b'iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAYAAABzenr0AAAABGdBTUEAALGPC/xhBQAAAAFzUkdCAK7OHOkAAAAgY0hSTQAAeiYAAICEAAD6AAAAgOgAAHUwAADqYAAAOpgAABdwnLpRPAAAAAZiS0dEAP8A/wD/oL2nkwAAAAlwSFlzAAAM8gAADPIBWg++mQAABgdJREFUWMO1l2tsHFcVx3/nzj68XttxTB71gzivhXjVvElC02wTkvRFKJumpQIVFKRKAQRSP0ARBQkhYSEhWlS1YGjFF0qVVtCgJqg1CpREMWnjtknTOHZCKgJJkyxZe+31Yze2d+cePowde2M7a0dwpJFWmtl7fnP+/3vuGWFcLNv8IMZagAeAX6tqDTMIBVdEXhXhmyipjpb9Rf/jG59cXBdEtlnVpwRqtm5cy8poBKu26EJGhFRPn/PH5sO7+jPZfcaYP0wH2ndD8u2qNDUsrY9cTnSyMhrh4c9uIe9ODyDRmeLgkXf8vQOZOWaaVTMFya023fmp5ZFv7X6YcGkJ1lryrsV13aJX3nWxrkXRmaiGz1gLIttUadq0bkXk23u+iOtarM5soVsNAzxgrf5y2dIFkS/Ft+O6lqtd3bjTKPv/InxAkzFSl7ia4ie/eBGriuta+geyBIMBmGFJZwygqnX3bt7A2uWfLHjrkmDA2wH2/wwAEI0sYsfWjeRdt+CmtYrexAsiMnKN/DaC3AqAVc/tqt5Co2GMjIDYAhAjggLp3n4SyRSDwzmMCMO5HK5VZoJxvREJcKL9HOcvXsGMg1CUlQ0RFi+oQVUxRrh4JUnzoWO0vt9BTzqF33FxLVj1kx4YRIxZJhCOxuKZYt3wOgAC7506y58PH8NxxtqIKoRDIZYurMVaOPpuG8/vPYCTu8J9q2FFvTCnQsjl4VJqmLf+IfztNF9PZ5jrGL4XjcUv3gxCGjZ9Xh9/7BEeum8LPb39ZAcHC0qoQEVZKeHSEG8db+OZ37zMnUvTfO0ew8fneJXTkQoiMJyD1g/h53+CM5d43THsAa5MBeEbn6hyVhlVlRVjJlSLtYqIcCmR5IW9B9i4JM33HzKEguBaJqjtGIg1QGUYvvsiO84necJv+E40FncngyjwwPHTYx64Ufvmw63Ya5fZc7fB78ALB+G2Soivgxt3qmth+QL48l3w09d4NO/yigitM/LAqPaRhXWk+wd479RZ7l0FC+fDG8fht4fgiZ2MaXBDWIV7VsGrx5jb/hFb/E4xAIVHdmzlc9s2Ish17VWVq53ddKW6uH2zYC0c/ADWLIb7V8NUJ7UqVJXBslpov8gawJlMhpt6wPWGE/ozWfL5YWaVem7v6oMtt0Mo4JV7qjACJX5AmA8EgGsz9kBkUR0+x0HE4Fpv0YDPg5hOkx55ZhCYFLW4BxbVMW/ObEpLy/l35yB3NAgr6qH5fdi1ASLVE004GoM5SPSAKmeAock6ZFEPuK7Lx2bP4hOL6zl8+io71zs8uAGOnvW8EKmePLljoOMjaLtAzjG8DdDe8trUAJN5QNWbb4LGYfumtfzsV200nxhi1x3C07u9qk12VonAwCC8dAS6B3jbMbw5lURTemAMTFkVjbBhdZS7Pr2eZ99ooTIMn1nu3bfjlBXAGOi/Bs81w1/byBjhaSBVFKD4WVDHV79wP/2ZLD/6/QnOXFZ2rBFqqsDveBXMDnllf+kIHGqn17UgwjrgA6AyGot3ALnxW3HaZ0GoJIgRoS+T5cBfjvL6m39H8t001FqqyrzteKETziUMvVlajfBDERqAJ4Fe0NkgjUATkB+FKPBARXmYivLwJIVSVMGqUh4u5dGddxNbv4KT7R/yzwsJLnWnKQkGqFkS4lzyOMLA70TMQeAUsDsQKl0dDJUykO5uVG8Cb4rG4vmOlv3jJBgxXbEYfaa+dj4L627DWiWfz+M4Dp3dad492U66r9+OiBhCtcwfCFI1vxafP1CeTiYa1ZvEm6KxeH663w8TwlpveFVVHMfxmpfVgu+CTMD9FyJPZvrSiZ5kgoqquVTOqy4XYxpR/Qbg890qwESRJkZ42MEEA/vs0BB9qeRzQPXsedUA5enkfxpVbdgHqBEjPueWiwF4c6JxTIGBO1r2E43FwefsI+8WQFg3X9HblfyBDzj/zsmOJcC0PkJvBpDq6WMgk80Zka4CiE1xAoT3DUuWvu7OZ611a9RrICrRWPwxVf0xUH3L2cdkcAXZK8LjQM/4/R6NxRFxUHW/gupTiPiBZ/4LnvL5JbtvU/gAAAAldEVYdGRhdGU6Y3JlYXRlADIwMjAtMDEtMDNUMDE6MDM6NTUrMDA6MDDezxEdAAAAJXRFWHRkYXRlOm1vZGlmeQAyMDIwLTAxLTAzVDAxOjAzOjU1KzAwOjAwr5KpoQAAAEZ0RVh0c29mdHdhcmUASW1hZ2VNYWdpY2sgNi43LjgtOSAyMDE5LTAyLTAxIFExNiBodHRwOi8vd3d3LmltYWdlbWFnaWNrLm9yZ0F74sgAAAAYdEVYdFRodW1iOjpEb2N1bWVudDo6UGFnZXMAMaf/uy8AAAAYdEVYdFRodW1iOjpJbWFnZTo6aGVpZ2h0ADUxMsDQUFEAAAAXdEVYdFRodW1iOjpJbWFnZTo6V2lkdGgANTEyHHwD3AAAABl0RVh0VGh1bWI6Ok1pbWV0eXBlAGltYWdlL3BuZz+yVk4AAAAXdEVYdFRodW1iOjpNVGltZQAxNTc4MDEzNDM1cTONFgAAABF0RVh0VGh1bWI6OlNpemUAMTlLQkKmYR+PAAAARXRFWHRUaHVtYjo6VVJJAGZpbGU6Ly8uL3VwbG9hZHMvMjU0MTYvM3l4VFhKZy8yMTA0L3NlYXJjaF9pY29uXzEyOTUzMy5wbmcOetOvAAAAAElFTkSuQmCC'

# FUNCTIONS
def extract_info_from_line(line):
    """Extracts time, job numbers, profile names, filenames, and filesizes from a log file line.

    Args:
        line: A single line from the log file.

    Returns:
        A list containing the extracted information if found, otherwise None.
    """
    # Patterns for extracting information
    time_pattern = r"\b(\d{2}:\d{2}:\d{2})\b"
    job_number_pattern = r"Job:\s+((?:\d+|GENERAL))"
    profilename_pattern = r"\[(.*?)\)]"
    filename_pattern = r"Start processing data of file '(.*?)'"
    filesize_pattern = r"length=(\d+),"

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
        return [time, job_numbers, profilenames, filenames, filesizes]
    else:
        return None


def process_file(filepath):
    """Processes a single log file.

    Args:
        filepath: Path to the log file.

    Returns:
        A list containing the extracted information from the log file.
    """
    matching_lines = []

    with open(filepath, "r", encoding="utf-8") as log_file:
        total_lines = sum(1 for _ in log_file)  # Get total lines in the file
        log_file.seek(0)  # Reset file pointer to start
        filename = os.path.basename(filepath)
        
        # Initialize tqdm progress bar for each line
        progress_bar = tqdm(total=total_lines, desc=f"Processing {os.path.basename(filepath)}", unit=" lines")
        window["-OUTPUT_WINDOW-"].print(f"File {filename} is being processed...")
        
        for line in log_file:
            # Extract information from the line
            extracted_info = extract_info_from_line(line)
            if extracted_info:
                matching_lines.append(extracted_info)
            progress_bar.update(1)  # Update progress bar for each line processed

        progress_bar.close()
        window["-OUTPUT_WINDOW-"].print(f"Processing {os.path.basename(filepath)} completed.")

    return matching_lines


def extract_and_write_to_csv(filepath, output_file_csv):
    """Extracts job numbers, profile names, filenames, and filesizes from log files and writes them to a CSV.

    Args: 
        filepath: The path to the log file or directory containing log files.
        output_file_csv (optional): The name of the output CSV file (defaults to "extracted_info.csv").
    """

    print("Starting app, please wait...")
    window["-OUTPUT_WINDOW-"].update("Starting app, please wait...\n\n")
    
    if os.path.isfile(filepath):
        files = [filepath]
    elif os.path.isdir(filepath):
        files = [os.path.join(filepath, f) for f in os.listdir(filepath) if f.endswith("_message.log")] # only log files that end with _message.log
    else:
        print("Invalid filepath.")
        return

    count = 0
    data = []
    
    for file in tqdm(files, desc="Processing Files"):
        count += 1
        window["-STATUSBAR-"].update(value = f"Processing: {count}/{len(files)} Files")
        data.extend(process_file(file))

    try:
        with open(output_file_csv, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["Time", "Job Number", "Profile Name", "Filename", "Filesize in Bytes"])  # Header row
            writer.writerows(data)
        print(f"Data has been written to {output_file_csv}\n\nTotal Matches: {len(data)}")
        window["-OUTPUT_WINDOW-"].update(f"Data has been written to {output_file_csv}\nTotal Matches: {len(data)}")
        window["-STATUSBAR-"].update(value=f"Processing complete")
    except Exception as e:
        print("Error writing to CSV:", e)
        window["-OUTPUT_WINDOW-"].update("Error writing to CSV:", e)


def print_total_log_files(filepath):
    try:
        if os.path.isfile(filepath):
            files = [filepath]
        elif os.path.isdir(filepath):
            files = [os.path.join(filepath, f) for f in os.listdir(filepath) if f.endswith("_message.log")] # only log files that end with _message.log
            window["-STATUSBAR-"].update(value=f"Total log files found: {len(files)}")
            window.refresh()
        else:
            window["-STATUSBAR-"].update(value="")
            pass
    except TypeError:
        pass


def summarize_filesize_in_bytes_column(folder_path_to_csv_result):
    try:
        window["-OUTPUT_WINDOW-"].update("Calculating Total Filesize in Bytes... please wait.")
        
        # Read the CSV file
        df = pd.read_csv(folder_path_to_csv_result) 

        # Calculate the total amount of numbers in the "Filesize in Bytes" column
        total_filesize = df['Filesize in Bytes'].sum()  

        kb = total_filesize / 1024
        mb = kb / 1024
        gb = mb / 1024  

        # Print the result
        window["-OUTPUT_WINDOW-"].update(f'Total Filesize in Bytes: {total_filesize}\nIn Kb = {round(kb,2)}\nIn Mb = {round(mb,2)}\nIn Gb = {round(gb,2)}')
    
    except FileNotFoundError:
        window["-OUTPUT_WINDOW-"].update(f"No such file or directory.")
        if values["-CSV_RESULT-"]:
            window["-OUTPUT_WINDOW-"].update("Grabbed folder path of input field of previous saved csv result.\n\nPlease retry by pressing SUM again.")
            window["-CSV_SUM_FILEPATH-"].update(values["-CSV_RESULT-"])
        else:
            window["-CSV_SUM_FILEPATH-"].update("")

custom_theme = {
    "BACKGROUND": "#31353d",
    "TEXT": "#FFFFFF",
    "INPUT": "#4d5157",
    "TEXT_INPUT": "#FFFFFF",
    "SCROLL": "#e2e3e3",
    "BUTTON": ("#000000", "#4E7AC7"),
    "PROGRESS": ("#4E7AC7", "#abacac"),
    "BORDER": 2,
    "SLIDER_DEPTH": 1,
    "PROGRESS_DEPTH": 1,
}

# Add your dictionary to the PySimpleGUI themes
sg.theme_add_new("MyTheme", custom_theme)
sg.theme("MyTheme")
font = ("Calibri", 13)

# CONSTANTS
CSV_FILETPYE = (("CSV (Comma Separated Value)", ".csv"),)

# ===== Layout ===== #
main_layout  = [[sg.Text("Lobster Message Log Searcher for Input Data", font="Calibri 15 bold", text_color="#466db3")],
                [sg.HSep()],
                [sg.Text("Select folder that contains log files:")],
                [sg.Input("", key="-LOGS_FILEPATH-", enable_events=True, expand_x=True , pad=5), sg.FolderBrowse(target="-LOGS_FILEPATH-", expand_x=True , pad=5)],
                [sg.Text("Select folder where to save result as CSV File:")],
                [sg.Input("", key="-CSV_RESULT-", expand_x=True, pad=5), sg.SaveAs(button_text="Browse", target="-CSV_RESULT-", file_types=CSV_FILETPYE, expand_x=True, pad=5)],
                [sg.Button("Start", key="-START-", size=(5,1)), sg.Button("Exit", key="-EXIT-", size=(5,1)), sg.Text("Status:") , sg.StatusBar(" ",key="-STATUSBAR-", auto_size_text=True, size=(10,1))]]

csv_summarize_filesize_layout = [[sg.Text("Sum Total Filesize:"), sg.Input("folder/path/to/csv_results.csv", key="-CSV_SUM_FILEPATH-", expand_x=True), sg.FileBrowse(file_types=CSV_FILETPYE, target="-CSV_SUM_FILEPATH-"), sg.Button("Summarize",key="-GET_SUM-")]]

output_window_layout = [[sg.Multiline(size=(42,12), key="-OUTPUT_WINDOW-")]]

checkbox_layout = [[sg.Checkbox("Show Sum Option", default=False, enable_events=True , key="-CSV_SUM_CHECKBOX-"), sg.pin(sg.Column(csv_summarize_filesize_layout, key="-CSV_SUM_FRAME-", visible=False))]]

layout = [[sg.Column(main_layout),sg.VSep(),sg.Column(output_window_layout)],
          [sg.Column(checkbox_layout)]]

window = sg.Window("Log Searcher", layout, font=font, icon=PROGRAM_ICON, finalize=True)

# Flag which checks the input element for every change
input_checked = False

while True:
    
    event,values = window.read()
    #print(f"Event {event} || Value {values}")
    
    if (event == sg.WINDOW_CLOSE_ATTEMPTED_EVENT or event == '-EXIT-') and sg.popup_yes_no('Do you really want to exit?') == 'Yes':
        break
    
    if event == sg.WIN_CLOSED:
        break
    
    try:
        # VARIABLES
        log_filepath = values["-LOGS_FILEPATH-"]
        output_csv = values["-CSV_RESULT-"]
        csv_result_path = values["-CSV_SUM_FILEPATH-"]
    except TypeError:
        pass
    
    if event == "-CSV_SUM_CHECKBOX-":
        if values["-CSV_SUM_CHECKBOX-"]:
            window["-CSV_SUM_FRAME-"].update(visible=True)
        else:
            window["-CSV_SUM_FRAME-"].update(visible=False)
    
    if event == "-LOGS_FILEPATH-":
        if not input_checked and len(log_filepath) > 0:
            input_checked = True
            window.perform_long_operation(lambda: print_total_log_files(log_filepath), "-OUTPUT_WINDOW-")
            input_checked = False
    
    if event == "-START-":
        if not log_filepath:
            window["-OUTPUT_WINDOW-"].update("Input for folder path that contains log files is empty.")
        elif not output_csv:
            window["-OUTPUT_WINDOW-"].update("Input folder path for saving result as CSV file is empty.")
        else:
            window.perform_long_operation(lambda: extract_and_write_to_csv(log_filepath,output_csv),"-OUTPUT_WINDOW-")
            
    if event == "-GET_SUM-":
        window.perform_long_operation(lambda: summarize_filesize_in_bytes_column(csv_result_path),"-OUTPUT_WINDOW-")
    
    
window.close()