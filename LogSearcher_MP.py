import PySimpleGUI as sg
from tqdm import tqdm
from multiprocessing import Pool
import csv
import re
import os
import re

program_icon = b'iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAYAAABzenr0AAAABHNCSVQICAgIfAhkiAAAAAlwSFlzAAAAvQAAAL0BHVrG+gAAABl0RVh0U29mdHdhcmUAd3d3Lmlua3NjYXBlLm9yZ5vuPBoAAAR3SURBVFiFrddZrN1TFAbw37qttklpipoqxCwSCTEkNcRYNdVYD6YoUWKMKagHJOLFkD6oeWpDa4h5aAlFgpiCENPVSklDKVpUjQ3Lw973+vf0ntN71Ur2wz77v/f61lrf/tY+MlNfA104Bx8iV3Pcga6+/AzW3g7B2ZiEjzp8tyq7GSdjUERMysy/m4tRo13JIuIJzMrM21fDuYiYjgUYj/ewAohOGdgA39ZDDsQ1A/D7Mw7OzGV1vgRj8SLubGaiE4CmvYrJAwHQcA4yc0lE7N8KYpUAImJjbIll+CAzfxoAEAoJuyqIxa0g+gQQESOwDu7BWliKeVg/IpbjXTyYmQ9HRBcux0aNI37GFZn5W3V2bURs01ifi1OwbCUSRsRRuBEfYDrewefYEAuxLQ7y7+04C4f2AeDWzPwrIkKp/84tcW6C/Vvv/nX4CadiFKbiY/xRxw+YhXMxXCHmQoxrpycddGYMuntLEBHn40hsj8PRjTkVzLzM/D4iRmPfGv3EOl7EAxGxM77D8WrN29jszFzQMxlcne9Ro98XR+ES7J2ZKwhQZi7EzIi4T1HJV3AYrsdMpa77rALAXEUXeg9V0zoFB+AbbNfPNB6vkHN4zcSFAy1B1JS/gtF4C9dk5oyamSGYhnUbEdyQmbN7JhHxQI3obVyQmbtFxLoKyfqyTzPzt4gYo5DcxRX9KHyNIS1Ix+HYxtigZX0PzMeailaso5Dzizbj2FYSbqvc673wVGb+2YSbmc+1iaTHXq/ORyqKOTYzL8Wlq9iHQsKtcVsFsAQiYs2amaEd9s7JzDmZ+XdEfIMdlCu5aX8c91gXfsEgBH4dyOaGDa7O18biiLglIrLNmNi6cS62wGeKwqmN5Mr+eI6I4UrU85SeMR8zcFmbLUtbAXyiCM95imaPyMzejyJinEKsHnspMxc15ocr5B2EzfFJZi7Hj/0JYDAeUkQolet4ptr76zWcaMVruBSzG+sXKa+eSXgBa0XEbO1b/dWZ+XAvgNqn71JSdjbejIh3KsH+xAkdApiiNJ67leZ1hlKCyR0AvLHCrN7J0ViE43CEkr6LtXlIYj08qZRvcyVzy/AMhg5ICXvacUTsgqeV5rMA9yrkehkvKSTbXekXuyjaPxmP1AD2xi1YQ2lqgauU90TT7snM13qUsDdNmfl2RJxUD56h6MJOOEYh2kh8hedxoqL/j2EE9snMLyNiAh7F45igKF8rgMUrlaCP9N5bUzoNR2NXbIY9cZpCwh/wPp7SkG9FvGbhWQzrdwlaLSLeVBg/UrnfXTWi+RXA/firRrscE3pkPCKG1t8T45vP8IgYppRsfUzvRJI3cGQ/yDSsRjtLg4A1E904veX7k2qZxqC708OhX5aZvyukG4THavQy8w/lpuzasqVL48Gy2gBaQHThyYjYMiK2UwjcHRFDImLHiNgR++HL/gBYpNRpoCC+wKfKi/k95YU9VuHE44qsT6lnL+pEwvGKRP+XP6ejFD34us36VrgJUzuR6//8e9465io9ZMg/PWr4aRCKlLwAAAAASUVORK5CYII='


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
main_layout  = [[sg.Text("Lobster Message Log Searcher for Input Data", font="Calibri 15 bold", text_color="#4E7AC7")],
                [sg.HSep()],
                [sg.Text("Select folder that contains log files:")],
                [sg.Input("", key="-LOGS_FILEPATH-", enable_events=True, expand_x=True , pad=5), sg.FolderBrowse(target="-LOGS_FILEPATH-", expand_x=True , pad=5)],
                [sg.Text("Save result as CSV File:")],
                [sg.Input("", key="-CSV_RESULT-", expand_x=True, pad=5), sg.SaveAs(button_text="Browse", target="-CSV_RESULT-", file_types=CSV_FILETPYE, expand_x=True, pad=5)],
                [sg.Button("Start", key="-START-", size=(5,1)), sg.Button("Exit", key="-EXIT-", size=(5,1)), sg.Text("Status:") , sg.StatusBar(" ",key="-STATUSBAR-", auto_size_text=True, size=(10,1))]]

output_window_layout = [[sg.Multiline(size=(42,12), key="-OUTPUT_WINDOW-")]]


layout = [[sg.Column(main_layout),sg.VSep(),sg.Column(output_window_layout)]]

window = sg.Window("Log Searcher", layout, font=font, icon=program_icon, finalize=True)

# Flag which checks the input element for every change
input_checked = False

while True:
    
    event,values = window.read()
    
    
    if (event == sg.WINDOW_CLOSE_ATTEMPTED_EVENT or event == '-EXIT-') and sg.popup_yes_no('Do you really want to exit?') == 'Yes':
        break
    
    if event == sg.WIN_CLOSED:
        break
    
    try:
        # VARIABLES
        log_filepath = values["-LOGS_FILEPATH-"]
        output_csv = values["-CSV_RESULT-"]
    except TypeError:
        pass
    
    if event == "-LOGS_FILEPATH-":
        if not input_checked and len(log_filepath) > 0:
            input_checked = True
            window.perform_long_operation(lambda: print_total_log_files(log_filepath), "-OUTPUT_WINDOW-")
            input_checked = False
    
    if event == "-START-":
        if not log_filepath:
            window["-OUTPUT_WINDOW-"].update("Input for folder path that contains log files is empty")
        elif not output_csv:
            window["-OUTPUT_WINDOW-"].update("Input folder path for saving result as CSV file is empty")
        else:
            window.perform_long_operation(lambda: extract_and_write_to_csv(log_filepath,output_csv),"-OUTPUT_WINDOW-")
    
    
window.close()