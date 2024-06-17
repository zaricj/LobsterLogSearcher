import os
import re
import csv
import PySimpleGUI as sg
import pywinstyles

# Function to search text using regex patterns and return matches
def regex_search(text, patterns):
    results = {pattern: re.findall(pattern, text) for pattern in patterns}
    return results

# Function to validate headers
def validate_headers(headers, num_patterns):
    return len(headers) == num_patterns


custom_theme = {
    "BACKGROUND": "#212121",
    "TEXT": "#FFFFFF",
    "INPUT": "#2f2f2f",
    "TEXT_INPUT": "#FFFFFF",
    "SCROLL": "#e2e3e3",
    "BUTTON": ("#27AE60", "#434343"),
    "PROGRESS": ("#27AE60", "#abacac"),
    "BORDER": 2,
    "SLIDER_DEPTH": 1,
    "PROGRESS_DEPTH": 1,
}

# Add your dictionary to the PySimpleGUI themes
sg.theme_add_new("MyTheme", custom_theme)
sg.theme("MyTheme")
font = ("Calibri", 13)

MENU_DEFINITION = [["&File", ["&Lobster TEST Logs Folder::OpenLogsFolder", "&Lobster PROD Logs Folder::OpenLogsFolder", "---", "&Clear Output::ClearOutput", "---", "E&xit::Exit"]],
                   ["&Help",["&How to Use::HowToUse"]],
                   ["&Expressions", ["&Time Pattern::TimePattern", "&Job Number Pattern::JobNumberPattern", "&Profilename Pattern::ProfilnamePattern", "&Filename Pattern::FilenamePattern", "&Filesize Pattern::FilesizePattern"]]]
MENU_RIGHT_CLICK_DELETE = ["&Right", ["&Delete", "&Delete All"]]
FRAME_TITLE_COLOR = "#27AE60"
PROGRAM_ICON = b'iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAYAAABzenr0AAAABGdBTUEAALGPC/xhBQAAAAFzUkdCAK7OHOkAAAAgY0hSTQAAeiYAAICEAAD6AAAAgOgAAHUwAADqYAAAOpgAABdwnLpRPAAAAAZiS0dEAP8A/wD/oL2nkwAAAAlwSFlzAAAM8gAADPIBWg++mQAABgdJREFUWMO1l2tsHFcVx3/nzj68XttxTB71gzivhXjVvElC02wTkvRFKJumpQIVFKRKAQRSP0ARBQkhYSEhWlS1YGjFF0qVVtCgJqg1CpREMWnjtknTOHZCKgJJkyxZe+31Yze2d+cePowde2M7a0dwpJFWmtl7fnP+/3vuGWFcLNv8IMZagAeAX6tqDTMIBVdEXhXhmyipjpb9Rf/jG59cXBdEtlnVpwRqtm5cy8poBKu26EJGhFRPn/PH5sO7+jPZfcaYP0wH2ndD8u2qNDUsrY9cTnSyMhrh4c9uIe9ODyDRmeLgkXf8vQOZOWaaVTMFya023fmp5ZFv7X6YcGkJ1lryrsV13aJX3nWxrkXRmaiGz1gLIttUadq0bkXk23u+iOtarM5soVsNAzxgrf5y2dIFkS/Ft+O6lqtd3bjTKPv/InxAkzFSl7ia4ie/eBGriuta+geyBIMBmGFJZwygqnX3bt7A2uWfLHjrkmDA2wH2/wwAEI0sYsfWjeRdt+CmtYrexAsiMnKN/DaC3AqAVc/tqt5Co2GMjIDYAhAjggLp3n4SyRSDwzmMCMO5HK5VZoJxvREJcKL9HOcvXsGMg1CUlQ0RFi+oQVUxRrh4JUnzoWO0vt9BTzqF33FxLVj1kx4YRIxZJhCOxuKZYt3wOgAC7506y58PH8NxxtqIKoRDIZYurMVaOPpuG8/vPYCTu8J9q2FFvTCnQsjl4VJqmLf+IfztNF9PZ5jrGL4XjcUv3gxCGjZ9Xh9/7BEeum8LPb39ZAcHC0qoQEVZKeHSEG8db+OZ37zMnUvTfO0ew8fneJXTkQoiMJyD1g/h53+CM5d43THsAa5MBeEbn6hyVhlVlRVjJlSLtYqIcCmR5IW9B9i4JM33HzKEguBaJqjtGIg1QGUYvvsiO84necJv+E40FncngyjwwPHTYx64Ufvmw63Ya5fZc7fB78ALB+G2Soivgxt3qmth+QL48l3w09d4NO/yigitM/LAqPaRhXWk+wd479RZ7l0FC+fDG8fht4fgiZ2MaXBDWIV7VsGrx5jb/hFb/E4xAIVHdmzlc9s2Ish17VWVq53ddKW6uH2zYC0c/ADWLIb7V8NUJ7UqVJXBslpov8gawJlMhpt6wPWGE/ozWfL5YWaVem7v6oMtt0Mo4JV7qjACJX5AmA8EgGsz9kBkUR0+x0HE4Fpv0YDPg5hOkx55ZhCYFLW4BxbVMW/ObEpLy/l35yB3NAgr6qH5fdi1ASLVE004GoM5SPSAKmeAock6ZFEPuK7Lx2bP4hOL6zl8+io71zs8uAGOnvW8EKmePLljoOMjaLtAzjG8DdDe8trUAJN5QNWbb4LGYfumtfzsV200nxhi1x3C07u9qk12VonAwCC8dAS6B3jbMbw5lURTemAMTFkVjbBhdZS7Pr2eZ99ooTIMn1nu3bfjlBXAGOi/Bs81w1/byBjhaSBVFKD4WVDHV79wP/2ZLD/6/QnOXFZ2rBFqqsDveBXMDnllf+kIHGqn17UgwjrgA6AyGot3ALnxW3HaZ0GoJIgRoS+T5cBfjvL6m39H8t001FqqyrzteKETziUMvVlajfBDERqAJ4Fe0NkgjUATkB+FKPBARXmYivLwJIVSVMGqUh4u5dGddxNbv4KT7R/yzwsJLnWnKQkGqFkS4lzyOMLA70TMQeAUsDsQKl0dDJUykO5uVG8Cb4rG4vmOlv3jJBgxXbEYfaa+dj4L627DWiWfz+M4Dp3dad492U66r9+OiBhCtcwfCFI1vxafP1CeTiYa1ZvEm6KxeH663w8TwlpveFVVHMfxmpfVgu+CTMD9FyJPZvrSiZ5kgoqquVTOqy4XYxpR/Qbg890qwESRJkZ42MEEA/vs0BB9qeRzQPXsedUA5enkfxpVbdgHqBEjPueWiwF4c6JxTIGBO1r2E43FwefsI+8WQFg3X9HblfyBDzj/zsmOJcC0PkJvBpDq6WMgk80Zka4CiE1xAoT3DUuWvu7OZ611a9RrICrRWPwxVf0xUH3L2cdkcAXZK8LjQM/4/R6NxRFxUHW/gupTiPiBZ/4LnvL5JbtvU/gAAAAldEVYdGRhdGU6Y3JlYXRlADIwMjAtMDEtMDNUMDE6MDM6NTUrMDA6MDDezxEdAAAAJXRFWHRkYXRlOm1vZGlmeQAyMDIwLTAxLTAzVDAxOjAzOjU1KzAwOjAwr5KpoQAAAEZ0RVh0c29mdHdhcmUASW1hZ2VNYWdpY2sgNi43LjgtOSAyMDE5LTAyLTAxIFExNiBodHRwOi8vd3d3LmltYWdlbWFnaWNrLm9yZ0F74sgAAAAYdEVYdFRodW1iOjpEb2N1bWVudDo6UGFnZXMAMaf/uy8AAAAYdEVYdFRodW1iOjpJbWFnZTo6aGVpZ2h0ADUxMsDQUFEAAAAXdEVYdFRodW1iOjpJbWFnZTo6V2lkdGgANTEyHHwD3AAAABl0RVh0VGh1bWI6Ok1pbWV0eXBlAGltYWdlL3BuZz+yVk4AAAAXdEVYdFRodW1iOjpNVGltZQAxNTc4MDEzNDM1cTONFgAAABF0RVh0VGh1bWI6OlNpemUAMTlLQkKmYR+PAAAARXRFWHRUaHVtYjo6VVJJAGZpbGU6Ly8uL3VwbG9hZHMvMjU0MTYvM3l4VFhKZy8yMTA0L3NlYXJjaF9pY29uXzEyOTUzMy5wbmcOetOvAAAAAElFTkSuQmCC'

listbox_element = []

# Layout for the GUI
menubar_layout = [[sg.MenubarCustom(MENU_DEFINITION, bar_background_color="#2f2f2f", bar_text_color="#FFFFFF",background_color="#4d5157",text_color="#FFFFFF")]]

main_layout = [[sg.Text("Select Log File:"), sg.InputText(key="-LOGS_FILEPATH-", enable_events=True, expand_x=True), sg.FileBrowse(file_types=(("Log Files", "*.log"),))],
    [sg.Text("RegEx Pattern:"), sg.InputText(key="-PATTERN-", expand_x=True), sg.Button("Add", key="-ADD-", expand_x=True)],
    [sg.Listbox(values=listbox_element, key="-PATTERNS-", right_click_menu=MENU_RIGHT_CLICK_DELETE, enable_events=True, expand_x=True, expand_y=True)],
    [sg.Text("Enter CSV Headers (comma separated):")],
    [sg.InputText(key="-HEADERS-", expand_x=True)],
    [sg.Button("Search and Save to CSV", key="-SEARCH-", expand_x=True)]
]

output_window_layout = [[sg.Multiline(size=(52,20), horizontal_scroll=True, expand_x=True, key="-OUTPUT_WINDOW-")]]

# GUI Frames

main_frame = sg.Frame("RegEx Searcher", main_layout, expand_x=True, expand_y=True,
                        title_color=FRAME_TITLE_COLOR, font="Calibri 13 bold")
output_window_frame = sg.Frame("Program Output", output_window_layout,expand_x=True, expand_y=True, title_color=FRAME_TITLE_COLOR, font="Calibri 13 bold")

layout = [[menubar_layout, main_frame, sg.VSep(), output_window_frame]]


# Create the window
window = sg.Window("RegEx Searcher", layout, font=font, icon=PROGRAM_ICON, finalize=True)

pywinstyles.apply_style(window, "dark")

# Event loop to process events

while True:
    event, values = window.read()
    
    if event == sg.WINDOW_CLOSED:
        break
    
    if event in ("Delete", "Delete All"):
        try:
            if event == "Delete":
                selected_indices = window["-PATTERNS-"].get_indexes()

                for index in selected_indices:
                    listbox_element.pop(index)
                    window["-PATTERNS-"].update(values=listbox_element)

            elif event == "Delete All":
                listbox_element.clear()
                window["-PATTERNS-"].update(values=listbox_element)

        except UnboundLocalError:
            window["-OUTPUT_WINDOW_MAIN-"].update("ERROR: To delete a filter from the Listbox, select it first.")
    
    # Add regex pattern to the list
    if event == "-ADD-":
        pattern = values["-PATTERN-"]
        if pattern:
            listbox_element.append(pattern)
            window["-PATTERNS-"].update(listbox_element)
            window["-PATTERN-"].update("")
    
    # Search text and save to CSV
    if event == "-SEARCH-":
        file_path = values["-LOGS_FILEPATH-"]
        headers = values["-HEADERS-"].split(",")
        
        if not file_path:
            window["-OUTPUT_WINDOW-"].update("Error: Please select a log file.")
            continue
        
        if validate_headers(headers, len(listbox_element)):
            # Read the contents of the selected log file
            try:
                with open(file_path, "r") as file:
                    text = file.read()
            except Exception as e:
                window["-OUTPUT_WINDOW-"].update(f"Error: {e}")
                continue
            
            results = regex_search(text, listbox_element)
            csv_data = list(zip(*results.values()))
            
            try:
                os.makedirs("result", exist_ok=True)
                with open("result/regex_matches.csv", mode="w", newline="") as file:
                    writer = csv.writer(file)
                    writer.writerow(headers)
                    writer.writerows(csv_data)
                
                window["-OUTPUT_WINDOW-"].update("Matches saved to regex_matches.csv")
            except Exception as e:
                window["-OUTPUT_WINDOW-"].update(f"Error: {e}")
        else:
            window["-OUTPUT_WINDOW-"].update("Error: Number of headers must match number of RegEx patterns")
            
    if event == "Clear Output::ClearOutput":
        window["-OUTPUT_WINDOW-"].update("")
        
    if event == "Lobster TEST Logs Folder::OpenLogsFolder":
        window.write_event_value(key="-LOGS_FILEPATH-", value="//nesist02/hub/logs/DataWizard")
        window["-LOGS_FILEPATH-"].update("//nesist02/hub/logs/DataWizard")
    
    if event == "Lobster PROD Logs Folder::OpenLogsFolder":
        window.write_event_value(key="-LOGS_FILEPATH-", value="//nesis002/hub/logs/DataWizard")
        window["-LOGS_FILEPATH-"].update("//nesis002/hub/logs/DataWizard")

    if event == "Time Pattern::TimePattern":
        window["-PATTERN-"].update(r"\b(\d{2}:\d{2}:\d{2})\b")
        window["-HEADERS-"].update("Time")
        
    if event == "Job Number Pattern::JobNumberPattern":
        window["-PATTERN-"].update(r"Job:\s+((?:\d+|GENERAL))")
        window["-HEADERS-"].update(window["-HEADERS-"].get() + ",Job Number")
        
    if event == "Profilename Pattern::ProfilnamePattern":
        window["-PATTERN-"].update(value=r"\[(.*?)]")
        window["-HEADERS-"].update(window["-HEADERS-"].get() + ",Profilename")
        
    if event == "Filename Pattern::FilenamePattern":
        window["-PATTERN-"].update(r"Start processing data of file '(.*?)'")
        window["-HEADERS-"].update(window["-HEADERS-"].get() + ",Filename")
        
    if event == "Filesize Pattern::FilesizePattern":
        window["-PATTERN-"].update(r"length=(\d+),")
        window["-HEADERS-"].update(window["-HEADERS-"].get() + ",Filesize in Bytes")
        
window.close()