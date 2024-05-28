import PySimpleGUI as sg
from tqdm import tqdm
import csv
import re
import os
import re

program_icon = b'iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAYAAABzenr0AAAABHNCSVQICAgIfAhkiAAAAAlwSFlzAAAAvQAAAL0BHVrG+gAAABl0RVh0U29mdHdhcmUAd3d3Lmlua3NjYXBlLm9yZ5vuPBoAAAR3SURBVFiFrddZrN1TFAbw37qttklpipoqxCwSCTEkNcRYNdVYD6YoUWKMKagHJOLFkD6oeWpDa4h5aAlFgpiCENPVSklDKVpUjQ3Lw973+vf0ntN71Ur2wz77v/f61lrf/tY+MlNfA104Bx8iV3Pcga6+/AzW3g7B2ZiEjzp8tyq7GSdjUERMysy/m4tRo13JIuIJzMrM21fDuYiYjgUYj/ewAohOGdgA39ZDDsQ1A/D7Mw7OzGV1vgRj8SLubGaiE4CmvYrJAwHQcA4yc0lE7N8KYpUAImJjbIll+CAzfxoAEAoJuyqIxa0g+gQQESOwDu7BWliKeVg/IpbjXTyYmQ9HRBcux0aNI37GFZn5W3V2bURs01ifi1OwbCUSRsRRuBEfYDrewefYEAuxLQ7y7+04C4f2AeDWzPwrIkKp/84tcW6C/Vvv/nX4CadiFKbiY/xRxw+YhXMxXCHmQoxrpycddGYMuntLEBHn40hsj8PRjTkVzLzM/D4iRmPfGv3EOl7EAxGxM77D8WrN29jszFzQMxlcne9Ro98XR+ES7J2ZKwhQZi7EzIi4T1HJV3AYrsdMpa77rALAXEUXeg9V0zoFB+AbbNfPNB6vkHN4zcSFAy1B1JS/gtF4C9dk5oyamSGYhnUbEdyQmbN7JhHxQI3obVyQmbtFxLoKyfqyTzPzt4gYo5DcxRX9KHyNIS1Ix+HYxtigZX0PzMeailaso5Dzizbj2FYSbqvc673wVGb+2YSbmc+1iaTHXq/ORyqKOTYzL8Wlq9iHQsKtcVsFsAQiYs2amaEd9s7JzDmZ+XdEfIMdlCu5aX8c91gXfsEgBH4dyOaGDa7O18biiLglIrLNmNi6cS62wGeKwqmN5Mr+eI6I4UrU85SeMR8zcFmbLUtbAXyiCM95imaPyMzejyJinEKsHnspMxc15ocr5B2EzfFJZi7Hj/0JYDAeUkQolet4ptr76zWcaMVruBSzG+sXKa+eSXgBa0XEbO1b/dWZ+XAvgNqn71JSdjbejIh3KsH+xAkdApiiNJ67leZ1hlKCyR0AvLHCrN7J0ViE43CEkr6LtXlIYj08qZRvcyVzy/AMhg5ICXvacUTsgqeV5rMA9yrkehkvKSTbXekXuyjaPxmP1AD2xi1YQ2lqgauU90TT7snM13qUsDdNmfl2RJxUD56h6MJOOEYh2kh8hedxoqL/j2EE9snMLyNiAh7F45igKF8rgMUrlaCP9N5bUzoNR2NXbIY9cZpCwh/wPp7SkG9FvGbhWQzrdwlaLSLeVBg/UrnfXTWi+RXA/firRrscE3pkPCKG1t8T45vP8IgYppRsfUzvRJI3cGQ/yDSsRjtLg4A1E904veX7k2qZxqC708OhX5aZvyukG4THavQy8w/lpuzasqVL48Gy2gBaQHThyYjYMiK2UwjcHRFDImLHiNgR++HL/gBYpNRpoCC+wKfKi/k95YU9VuHE44qsT6lnL+pEwvGKRP+XP6ejFD34us36VrgJUzuR6//8e9465io9ZMg/PWr4aRCKlLwAAAAASUVORK5CYII='


# FUNCTIONS

def extract_info_from_log(path_to_folder):
    """Extracts time, job numbers, profile names, filenames, and filesizes from a log file.

    Args:
        log_file: The log file object.

    Returns:
        A list containing the extracted information.
    """
    all_matches = []
    
    global total_matches
    total_matches = 0
    # Progress Bar
    total_files = sum(1 for filename in os.listdir(path_to_folder) if filename.endswith('.log'))
    progress_increment = 100 / total_files
    current_progress = 0
    window['-PROGRESS_BAR-'].update(current_progress)

    # Patterns for extracting information
    time_pattern = r"\b(\d{2}:\d{2}:\d{2})\b"
    job_number_pattern = r"Job:\s+((?:\d+|GENERAL))"
    profilename_pattern = r"\[(.*?)\)]"
    filename_pattern = r"Start processing data of file '(.*?)'"
    filesize_pattern = r"length=(\d+),"
    log_filename_pattern = r"\d{4}_\d{2}_\d{2}_message\.log" # Only files that are called yyyy_mm_dd_message.log
        
    # Loop through all files in the folder
    for filename in os.listdir(path_to_folder):
        if filename.endswith(".log"):  # Check if it's a log file
            if re.match(log_filename_pattern,filename):
                filepath = os.path.join(path_to_folder, filename)
                # Progress Bar
                current_progress += progress_increment
                window['-PROGRESS_BAR-'].update(round(current_progress, 2))
                window["-OUTPUT_WINDOW-"].print(f"Processing {filename}")

                with open(filepath, "r", encoding="utf-8") as log_file:
                    matching_lines = []
                    for line in tqdm(log_file, desc=f"Processing {filename}"):

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

                    all_matches.extend(matching_lines)
                    total_matches += len(matching_lines)
    
    window["-OUTPUT_WINDOW-"].print(f"Total matches found in file {filename}:", total_matches)  # Print the total number of matches found
    return all_matches


def extract_and_write_to_csv(folder_path, output_file_csv="extracted_info.csv"):
    """Extracts job numbers, profile names, filenames, and filesizes from log files in a folder
    and writes them to a CSV.

    Args: 
        folder_path: The path to the folder containing log files.
        output_file_csv (optional): The name of the output CSV file (defaults to "extracted_info.csv").
    """
    
    data = extract_info_from_log(folder_path)

    try:
        with open(output_file_csv, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["Time", "Job Number", "Profile Name", "Filename", "Filesize in Bytes"])  # Header row
            writer.writerows(data)
        window["-OUTPUT_WINDOW-"].update(f"Data has been written to {output_file_csv}")
        window["-OUTPUT_WINDOW-"].print(f"Total Matches: {total_matches}")
        
    except Exception as e:
        window["-OUTPUT_WINDOW-"].update("Error writing to CSV:", e)
        window['-PROGRESS_BAR-'].update(0)


custom_theme = {
    "BACKGROUND": "#F4F4F4",
    "TEXT": "#000000",
    "INPUT": "#bbbdbd",
    "TEXT_INPUT": "#000000",
    "SCROLL": "#e2e3e3",
    "BUTTON": ("#000000", "#FFB948"),
    "PROGRESS": ("#FFB948", "#abacac"),
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
main_layout  = [[sg.Text("Lobster log searcher for input data and it's size", font="Calibri 15 bold underline")],
                [sg.HSep()],
                [sg.Text("Folder that contains log files:")],
                [sg.Input(" ", size=(35,1), key="-LOGS_FILEPATH-"), sg.FolderBrowse(target="-LOGS_FILEPATH-")],
                [sg.Text("Save result as CSV File:")],
                [sg.Input(" ", size=(35,1), key="-CSV_RESULT-"), sg.SaveAs(target="-CSV_RESULT-", file_types=CSV_FILETPYE)]]

output_window_layout = [[sg.Multiline(size=(30,12), key="-OUTPUT_WINDOW-")]]

progress_bar_layout = [[sg.Text(("Progress: ")), sg.ProgressBar(100,orientation="horizontal", size=(40,17), key="-PROGRESS_BAR-"), sg.Button("Start", key="-START-", size=(5,1)), sg.Button("Exit", key="-EXIT-", size=(5,1))]]

layout = [[sg.Column(main_layout),sg.VSep(),sg.Column(output_window_layout)],
          [sg.Column(progress_bar_layout, justification="center")]]

window = sg.Window("Log Searcher", layout, font=font, icon=program_icon, finalize=True)

while True:
    
    event,values = window.read()
    
    if (event == sg.WINDOW_CLOSE_ATTEMPTED_EVENT or event == '-EXIT-') and sg.popup_yes_no('Do you really want to exit?') == 'Yes':
        break

    
    # VARIABLES
    log_filepath = values["-LOGS_FILEPATH-"] 
    output_csv = values["-CSV_RESULT-"]
    
    if event == "-START-":
        if not log_filepath:
            window["-OUTPUT_WINDOW-"].update("Input for folder path that contains log files is empty")
        elif not output_csv:
            window["-OUTPUT_WINDOW-"].update("Input folder path for saving result as CSV file is empty")
        else:
            window.perform_long_operation(lambda: extract_and_write_to_csv(log_filepath,output_csv),"-OUTPUT_WINDOW-")
    
window.close()