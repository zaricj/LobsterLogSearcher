import csv
import os
import re
import sys
import pandas as pd
import datetime
from PySide6.QtCore import QSettings, QThread, Signal, Qt
from PySide6.QtGui import QAction, QCloseEvent, QIcon
from PySide6.QtWidgets import (
    QApplication,
    QFileDialog,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMenu,
    QProgressBar,
    QPushButton,
    QStatusBar,
    QTextEdit,
    QVBoxLayout,
    QWidget,
    QTabWidget,
    QMessageBox,
)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Constants
STYLESHEET_THEME = """
QWidget {
    background-color: #232428;
    color: #ffffff;
    border-radius: 4px;
}

QMenuBar {
    background-color: #232428;
    padding: 4px;
}

QMenuBar::item:selected {
    background-color: #ffc857;
    border-radius: 4px;
    color: #1e1e1e;
}

QMenu {
    background-color: #232428;
    border: 1px solid #313338;
    border-radius: 2px;
}

QMenu::item:selected {
    background-color: #ffc857;
    color: #1e1e1e;
}

QPushButton {
    background-color: #ffc857;
    color: #000000;
    border: none;
    padding: 4px 12px;
    border-radius: 4px;
    font: bold;
}

QPushButton:hover {
    background-color: #e2b04f;
}

QPushButton:pressed {
    background-color: #a3803c;
}

QPushButton:disabled {
    background-color: #808080;
}

QPushButton#export_to_excel {
    background-color: #21a366;
    color: #000000;
    border: none;
    padding: 4px 12px;
    border-radius: 4px;
    font: bold;
}

QPushButton#export_to_excel:hover {
    background-color: #228b58;
}

QPushButton#export_to_excel:pressed {
    background-color: #21754b;
}

QPushButton#export_to_excel_summary {
    background-color: #21a366;
    color: #000000;
    border: none;
    padding: 4px 12px;
    border-radius: 4px;
    font: bold;
}

QPushButton#export_to_excel_summary:hover {
    background-color: #228b58;
}

QPushButton#export_to_excel_summary:pressed {
    background-color: #21754b;
}

QLineEdit, QComboBox, QTextEdit {
    background-color: #313338;
    border: 1px solid #4a4a4a;
    padding: 4px 12px;
    border-radius: 4px;
}

QStatusBar {
    background-color: #313338;
    border: 1px solid #4a4a4a;
    padding: 4px 12px;
    border-radius: 4px;
    font-size: 16px;
    font: bold;
    color: #20df85;
}

QListWidget {
    background-color: #313338;
}

QListWidget::item {
    height: 26px;
}

QListWidget::item:selected {
    background-color: #ffc857;
    color: #000000;
}

QComboBox::drop-down {
    border: none;
    background-color: #ffc857;
    width: 20px;
    border-top-right-radius: 6px;
    border-bottom-right-radius: 6px;
}

QComboBox:disabled {
    background-color: #808080;    
}

QComboBox::drop-down:disabled {
    border: none;
    background-color: #3d3d3d;
}

QTabWidget::pane {
    border: 1px solid #313338;
    border-radius: 4px;
}

QTabBar::tab {
    background-color: #232428;
    border: 1px solid #313338;
    padding: 4px 12px;;
    border-radius: 4px;
    margin: 0 2px;
}

QTabBar::tab:selected {
    background-color: #ffc857;
    color: #000000;
}

QTableView {
    background-color: #313338;
}

QGroupBox {
    border: 1px solid #313338;
    border-radius: 4px;
    margin-top: 10px;
    padding: 10px;
    font: bold;
    color: #ffc857;
}

QGroupBox::title {
    subcontrol-origin: margin;
    subcontrol-position: top left;
    left: 12px;
    padding: 0 6px;
}

QRadioButton::indicator {
    width: 16px;
    height: 16px;
}

QRadioButton:disabled {
    color: #808080; 
}

QScrollBar:vertical {
    border: none;
    background: #232428;
    width: 12px;
    margin: 4px;
    border-radius: 4px;
}

QScrollBar::handle:vertical {
    background: #313338;
    min-height: 20px;
    border-radius: 4px;
}

QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
    height: 0px;
}

/* Progress Bar */
QProgressBar {
    background-color: #313338;
    border: 1px solid #4a4a4a;
    border-radius: 4px;
    text-align: center;
    color: #ffffff;
    padding: 1px 2px;
}

QProgressBar::chunk {
    background-color: #21a366;
}

/* CheckBox */
QCheckBox {
    spacing: 6px;
}

QCheckBox::indicator {
    width: 16px;
    height: 16px;
    border-radius: 4px;
    background-color: #313338;
    border: 1px solid #4a4a4a;
}

QCheckBox::indicator:checked {
    background-color: #ffc857;
    border: 1px solid #ec971f;
}

QCheckBox::indicator:unchecked {
    background-color: #313338;
    border: 1px solid #4a4a4a;
}
"""


class GenericWorker(QThread):
    output_window = Signal(str)
    status = Signal(str)
    finished = Signal(str)
    progress_value = Signal(int)
    cancel_requested = Signal()
    messagebox_warn = Signal(str, str)
    messagebox_info = Signal(str, str)
    messagebox_crit = Signal(str, str)
    output_window_clear = Signal()
    statistics_ready = Signal(dict) # New signal to emit generated statistics

    def __init__(self, task:str, *args, **kwargs):
        super().__init__()
        self.task = task  # A string to identify the task: "export_excel" or "search_logs" or "generate_statistics"
        self.args = args  # Arguments for the task
        self.kwargs = kwargs # Keyword arguments for the task
        self._is_cancelled = False
        self.cancel_requested.connect(self.cancel)
        self.pattern = re.compile(r"\b(\d{2}:\d{2}:\d{2})\b\s+NORMAL\s+Job:\s+(GENERAL|\d+)\s+\[(.*?)\].*?Start processing data of file '(.+?)'@.*?length=(\d+)")
        self.statistics_dataframes = {} # Initialize dictionary to hold dataframes

    def cancel(self):
        self._is_cancelled = True


    def run(self):
        if self.task == "export_excel":
            self.export_csv_to_excel(*self.args, **self.kwargs)
        elif self.task == "write_log_data_to_csv":
            self.extract_and_write_to_csv(*self.args, **self.kwargs)
        elif self.task == "generate_statistics": # New task for statistics generation
            self.generate_and_emit_statistics(*self.args, **self.kwargs)
        else:
            self.messagebox_crit.emit("Unknown Task", f"Unknown task: {self.task}")


    def export_csv_to_excel(self, csv_file_path:str, excel_file_path:str) -> None:
        if not csv_file_path:
            self.messagebox_warn.emit(
                "No CSV File", "Please select a CSV file to convert."
            )
            return
        else:
            self.output_window.emit("Exporting CSV to Excel... please wait.")

        try:
            # Use 'utf-8-sig' to handle BOM if present, and pandas sniffer for delimiter
            csv_df = pd.read_csv(
                csv_file_path,
                encoding="utf-8",
                engine="pyarrow",
            )
            csv_row_count = csv_df.shape[0]
            
            if csv_row_count < 1048576:
                # Export to Excel
                with pd.ExcelWriter(excel_file_path, engine="xlsxwriter") as writer:
                    csv_df.to_excel(writer, sheet_name="Statistics Data", index=False)

                self.messagebox_info.emit(
                    "Successful conversion",
                    f"Successfully converted:\n{csv_file_path}\nto\n{excel_file_path}",
                )
            else:
                self.messagebox_warn.emit("Too many rows", f"The CSV file exceeds the maximum row limit 1048576 for Excel. Your file contains {csv_row_count} rows.\nFile conversion not possible.")
            
            self.output_window_clear.emit()

        except FileNotFoundError:
            self.messagebox_crit.emit(
                "File Not Found", f"The file '{csv_file_path}' was not found."
            )
            self.output_window_clear.emit()
        except pd.errors.EmptyDataError:
            self.messagebox_warn.emit(
                "Empty CSV", "The selected CSV file is empty."
            )
            self.output_window_clear.emit()
        except Exception as ex:
            message = f"An exception of type {type(ex).__name__} occurred. Arguments: {ex.args!r}"
            self.messagebox_crit.emit(
                "Exception exporting CSV", f"Error exporting CSV: {message}"
            )
            self.output_window_clear.emit()


    def process_file(self, filepath:str, file_index:int, total_files:int, writer) -> int:
        if self._is_cancelled:
            return 0

        total_lines = self.count_lines(filepath)
        if total_lines == 0:
            self.output_window.emit(f"Skipping empty file: {filepath}")
            return 0

        filename_for_output = os.path.basename(filepath)
        self.output_window.emit(
            f"Processing {filename_for_output}... (Filesize: {os.path.getsize(filepath)} bytes, {total_lines} lines)"
        )
        self.progress_value.emit(
            int((file_index / total_files) * 100)
        )  # High-level file progress

        # Extracting date from filename like 13_05_2025_message.log -> 13_05_2025
        filename_prefix = filename_for_output.split('_message.log')[0] if '_message.log' in filename_for_output else filename_for_output

        matches_in_file = 0
        with open(filepath, "r", encoding="utf-8", errors="ignore") as log_file:
            for idx, line in enumerate(log_file, start=1):
                if self._is_cancelled:
                    break

                extracted_info = self.extract_info_from_line(line, filename_prefix)
                if extracted_info:
                    writer.writerow(extracted_info)
                    matches_in_file += 1

                if (
                    idx % 1000 == 0 or idx == total_lines
                ):  # Update progress every 1000 lines or at the end
                    percent = int((idx / total_lines) * 100)
                    self.status.emit(
                        f"Processing file {file_index}/{total_files} - Current file progress: {percent}% completed"
                    )

        self.output_window.emit(f">>> Finished processing {filename_for_output}.")
        return matches_in_file


    def extract_and_write_to_csv(self, filepath:str, output_file_csv:str) -> None:
        files = []
        if os.path.isfile(filepath):
            if filepath.endswith("_message.log"):
                files = [filepath]
            else:
                self.output_window.emit("Selected file is not a '_message.log' file. Please select a valid log file or a directory containing them.")
                self.finished.emit("Processing failed.")
                return
        elif os.path.isdir(filepath):
            files = [
                os.path.join(filepath, f)
                for f in os.listdir(filepath)
                if f.endswith("_message.log") and os.path.isfile(os.path.join(filepath, f))
            ]
            if not files:
                self.output_window.emit("No '_message.log' files found in the selected directory.")
                self.finished.emit("Processing failed.")
                return
        else:
            self.output_window.emit("Invalid filepath. Please select a valid file or directory.")
            self.finished.emit("Processing failed.")
            return

        total_matches = 0

        if not self._is_cancelled:
            try:
                with open(
                    output_file_csv, "w", newline="", encoding="utf-8"
                ) as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(
                        [
                            "Time",
                            "Job ID",
                            "Profile Name",
                            "Filename",
                            "Filesize in Bytes",
                        ]
                    )

                    for idx, file in enumerate(files, start=1):
                        if self._is_cancelled:
                            self.output_window.emit("Operation cancelled by user.")
                            break
                        matches_in_file = self.process_file(file, idx, len(files), writer)
                        total_matches += matches_in_file

                if not self._is_cancelled: # Only emit finished if not cancelled
                    self.finished.emit(
                        f"\nData written to: {output_file_csv}\n"
                        f"Total Log Files Processed: {len(files)}\n"
                        f"Total Matches Found: {total_matches}"
                    )
                else: # if cancelled during the process, it should state that it is cancelled
                    self.finished.emit(
                        f"Operation cancelled.\nData partially written to: {output_file_csv}\n"
                        f"Total Log Files Processed: {idx-1}/{len(files)}\n" # idx-1 because idx increments before the loop for current file
                        f"Total Matches Found: {total_matches}"
                    )

            except Exception as e:
                self.messagebox_crit.emit(
                    "Error writing to CSV",
                    f"An error occurred while writing to CSV: {e}"
                )
                self.finished.emit("Processing failed due to an error.")
        else:
            self.finished.emit("Operation cancelled by user.")


    # Helper methods
    def count_lines(self, filepath:str) -> int:
        count = 0
        try:
            with open(filepath, "r", encoding="utf-8", errors="ignore") as file:
                for _ in file:
                    count += 1
        except Exception as e:
            self.output_window.emit(f"Error counting lines in {filepath}: {e}")
            return 0
        return count


    def extract_info_from_line(self, line:str, date_prefix:str) -> list:
        match = self.pattern.search(line)
        if match:
            # Combining date prefix with the time extracted from the log line
            full_timestamp = f"{date_prefix} {match.group(1)}"
            return [full_timestamp, match.group(2), match.group(3), match.group(4), int(match.group(5))]
        return None

    def generate_and_emit_statistics(self, csv_path:str) -> None:
        try:
            self.output_window.emit("Generating summary statistics... please wait.")
            self.create_summary(csv_path)
            self.output_window.emit("Generating profile size analysis... please wait.")
            self.create_profile_size_analysis(csv_path)
            self.statistics_ready.emit(self.statistics_dataframes)
            self.finished.emit("Statistics generation complete.")
        except Exception as e:
            self.messagebox_crit.emit("Statistics Generation Error", f"An error occurred during statistics generation: {e}")
            self.finished.emit("Statistics generation failed.")


    def create_summary(self, csv_path:str) -> None:
        try:
            usecols = ["Filename", "Filesize in Bytes", "Profile Name"]
            df = pd.read_csv(csv_path, usecols=usecols, engine="pyarrow")

            size_stats = df["Filesize in Bytes"].agg(["sum", "mean", "max", "min", "median", "std"])
            total_files = df["Filename"].nunique()
            total_entries = len(df)
            total_size = size_stats["sum"]
            avg_size = size_stats["mean"]
            max_size = size_stats["max"]
            min_size = size_stats["min"]
            median_size = size_stats["median"]
            std_dev_size = size_stats["std"]
            # Store summary data for export (this is generally small enough)
            summary_data = {
                "Metric": ["Unique Files Total", "Total Log Entries", "Total Size (Bytes)", "Avg File Size (Bytes)", 
                           "Largest File Size (Bytes)", "Smallest File Size (Bytes)", "Median File Size (Bytes)", 
                           "Std Dev File Size (Bytes)"],
                "Value": [total_files, total_entries, total_size, avg_size, max_size, min_size, median_size, std_dev_size]
            }
            self.statistics_dataframes["Summary Statistics"] = pd.DataFrame(summary_data)

        except FileNotFoundError:
            self.messagebox_crit.emit("File Not Found", f"CSV file '{csv_path}' for summary not found.")
        except pd.errors.EmptyDataError:
            self.messagebox_warn.emit("Empty CSV", "The selected CSV file for summary is empty.")
        except KeyError:
            self.messagebox_crit.emit("Column Missing", "Required column 'Filesize in Bytes' or 'Filename' or 'Profile Name' not found in CSV for summary.")
        except Exception as e:
            self.messagebox_crit.emit("Error creating summary", f"An exception occurred during summary creation: {e}")


    def create_profile_size_analysis(self, csv_path:str) -> None:
        try:
            df = pd.read_csv(csv_path, usecols=["Filename", "Filesize in Bytes", "Profile Name"], engine="pyarrow")

            profile_size_summary = (
                df.groupby("Profile Name")
                .agg(
                    {
                        "Filesize in Bytes": ["mean", "std", "min", "max", "count"],
                        "Filename": lambda x: list(dict.fromkeys(filter(None, x)))[:5],
                    }
                )
                .reset_index()
            )
            profile_size_summary.columns = [
                "Profile Name",
                "Avg File Size (Bytes)",
                "Std File Size (Bytes)",
                "Min File Size (Bytes)",
                "Max File Size (Bytes)",
                "Count",
                "Filenames Sample",
            ]
            profile_size_summary["Avg File Size (Bytes)"] = profile_size_summary["Avg File Size (Bytes)"].round(2)
            profile_size_summary["Std File Size (Bytes)"] = profile_size_summary["Std File Size (Bytes)"].round(2)
            profile_size_summary["Filenames Sample"] = profile_size_summary["Filenames Sample"].apply(lambda x: ", ".join(x) if x else "None")

            # Store for export
            self.statistics_dataframes["Per Profile Statistics"] = profile_size_summary

        except FileNotFoundError:
            self.messagebox_crit.emit("File Not Found", f"CSV file '{csv_path}' for profile analysis not found.")
        except pd.errors.EmptyDataError:
            self.messagebox_warn.emit("Empty CSV", "The selected CSV file for profile analysis is empty.")
        except KeyError:
            self.messagebox_crit.emit("Column Missing", "Required column 'Filesize in Bytes' or 'Filename' or 'Profile Name' not found in CSV for profile analysis.")
        except Exception as e:
            self.messagebox_crit.emit("Error creating profile size analysis", f"An exception occurred during profile size analysis: {e}")


class LogSearcherGUI(QMainWindow):
    
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Lobster Message Log Searcher v1.0")
        self.setWindowIcon(QIcon(os.path.join(SCRIPT_DIR, "_internal", "icon", "wood.ico")))
        # Initialize settings for window geometry
        self.settings = QSettings(
            "Application", "Name"
        )  # Settings to save current location of the windows on exit
        geometry = self.settings.value("app_geometry", bytes())

        # Load recent folders
        self.recent_folders = self.settings.value("recent_folders", [], type=list)

        self.central_widget = QWidget()
        self.setCentralWidget(self.central_widget)
        self.layout = QVBoxLayout(self.central_widget)
        self.layout.setContentsMargins(10, 10, 10, 10)
        self.restoreGeometry(geometry)

        self.setup_ui()
        self.apply_stylesheet()


    def setup_ui(self):
        self.setup_menubar()

        # Log Processing Input Group
        input_group = QGroupBox("Log Processing Input")
        input_layout = QVBoxLayout(input_group)

        input_layout.addWidget(QLabel("Log Files Location"))
        self.log_filepath_layout = QHBoxLayout()
        self.log_filepath_input = QLineEdit()
        self.log_filepath_input.setPlaceholderText(
            "Select a folder that contains lobster message logs..."
        )
        self.log_filepath_button = QPushButton("Browse")
        self.log_filepath_button.clicked.connect(self.browse_log_folder)
        self.log_filepath_layout.addWidget(self.log_filepath_input)
        self.log_filepath_layout.addWidget(self.log_filepath_button)
        input_layout.addLayout(self.log_filepath_layout)

        input_layout.addWidget(QLabel("Output CSV Location"))
        self.csv_result_layout = QHBoxLayout()
        self.csv_result_input = QLineEdit()
        self.csv_result_input.setPlaceholderText(
            "Select a folder where to save the CSV result file..."
        )
        self.csv_result_button = QPushButton("Browse")
        self.csv_result_button.clicked.connect(self.browse_csv_save)
        self.csv_result_layout.addWidget(self.csv_result_input)
        self.csv_result_layout.addWidget(self.csv_result_button)
        input_layout.addLayout(self.csv_result_layout)

        controls_layout = QHBoxLayout()
        self.start_button = QPushButton("Start Processing")
        self.start_button.clicked.connect(self.start_processing)
        self.cancel_button = QPushButton("Cancel")
        self.cancel_button.clicked.connect(self.cancel_processing)
        self.cancel_button.setEnabled(False)
        controls_layout.addWidget(self.start_button)
        controls_layout.addWidget(self.cancel_button)
        input_layout.addLayout(controls_layout)

        self.layout.addWidget(input_group)

        # Log Processing Output Group
        output_group = QGroupBox("Log Processing Output")
        output_layout = QVBoxLayout(output_group)

        self.status_bar = QStatusBar()
        self.status_bar.setSizeGripEnabled(False)

        self.progress_bar = QProgressBar()
        self.progress_bar.setValue(0)
        output_layout.addWidget(self.progress_bar)

        self.program_output_window = QTextEdit()
        self.program_output_window.setReadOnly(True)

        # Add widgets to output layout
        output_layout.addWidget(self.program_output_window)
        output_layout.addWidget(self.status_bar)

        self.layout.addWidget(output_group)

        # File Size Summary Group (Modified to only contain CSV selection and export buttons)
        summary_group = QGroupBox("Statistics Export") # Renamed group
        summary_layout = QVBoxLayout(summary_group)

        summary_layout.addWidget(
            QLabel("Select CSV File to Generate and Export Statistics")
        )
        self.csv_sum_layout = QHBoxLayout()
        self.csv_sum_filepath_input = QLineEdit()
        self.csv_sum_filepath_input.setPlaceholderText(
            "Select a CSV file to generate statistics from..."
        )
        self.csv_sum_filepath_button = QPushButton("Browse")
        self.csv_sum_filepath_button.clicked.connect(self.browse_csv_sum)
        
        self.export_to_excel = QPushButton("Export CSV to Excel") # Kept original CSV to Excel
        self.export_to_excel.setObjectName("export_to_excel")
        self.export_to_excel.clicked.connect(self.export_to_excel_triggered)
        
        # Modified button for statistics generation and export
        self.export_summary_statistics_excel = QPushButton("Generate and Export Statistics to Excel")
        self.export_summary_statistics_excel.setHidden(False)  # Now visible
        self.export_summary_statistics_excel.clicked.connect(self.start_statistics_generation) # Connected to new method

        self.csv_sum_layout.addWidget(self.csv_sum_filepath_input)
        self.csv_sum_layout.addWidget(self.csv_sum_filepath_button)
        self.csv_sum_layout.addWidget(self.export_to_excel) # Add this back if it was removed in thought, it's original purpose was different
        summary_layout.addLayout(self.csv_sum_layout)
        summary_layout.addWidget(self.export_summary_statistics_excel) # Add the new button


        self.layout.addWidget(summary_group)
    
    # This method is removed as per the plan
    # def summarize_filesize(self) -> None:
    #     # ... (removed content)

    def apply_stylesheet(self):
        self.setStyleSheet(STYLESHEET_THEME)


    def setup_menubar(self):
        menubar = self.menuBar()

        file_menu = menubar.addMenu("&File")

        self.recent_menu = QMenu("Recent Folders", self)
        self.recent_menu.clear()

        for folder in self.recent_folders:
            action = QAction(folder, self)
            action.triggered.connect(lambda checked, p=folder: self.open_logs_folder(p))
            self.recent_menu.addAction(action)

        file_menu.addMenu(self.recent_menu)

        file_menu.addSeparator()

        clear_recent_action = QAction("Clear Recent Folders", self)
        clear_recent_action.triggered.connect(self.clear_recent_folders)
        file_menu.addAction(clear_recent_action)

        file_menu.addSeparator()

        clear_output_action = QAction("Clear Output", self)
        clear_output_action.triggered.connect(self.clear_all_output)
        file_menu.addAction(clear_output_action)

        file_menu.addSeparator()

        exit_action = QAction("Exit", self)
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)
        
        open_menu = menubar.addMenu("&Open")
        open_csv_output = QAction("Open CSV Output Folder", self)

        open_csv_output.triggered.connect(self.open_output_csv_folder)
        
        open_menu.addAction(open_csv_output)

        presets_menu = menubar.addMenu("&Presets")
        open_test_logs = QAction("//nesist02/hub/logs/DataWizard", self)
        open_test_logs.triggered.connect(
            lambda: self.open_logs_folder("//nesist02/hub/logs/DataWizard")
        )
        presets_menu.addAction(open_test_logs)
        open_prod_logs = QAction("//nesis002/hub/logs/DataWizard", self)
        open_prod_logs.triggered.connect(
            lambda: self.open_logs_folder("//nesis002/hub/logs/DataWizard")
        )
        presets_menu.addAction(open_prod_logs)
        help_menu = menubar.addMenu("&Help")

        about_action = QAction("About", self)
        about_action.triggered.connect(self.show_about)
        help_menu.addAction(about_action)

    def open_output_csv_folder(self) -> None:
        output_path = self.csv_result_input.text()
        if output_path and os.path.exists(os.path.dirname(output_path)):
            try:
                os.startfile(os.path.dirname(output_path))
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Could not open folder: {e}")
        else:
            QMessageBox.warning(self, "Invalid Path", "The output CSV path is not valid or does not exist.")


    def browse_log_folder(self):
        folder = QFileDialog.getExistingDirectory(self, "Select Log Folder")
        if folder:
            self.log_filepath_input.setText(folder)
            self.add_recent_folder(folder)
            self.print_total_log_files(folder)
        else:
            self.status_bar.clearMessage() # Clear status bar if selection is cancelled


    def browse_csv_save(self):
        # Suggest a default filename based on the log folder, if available
        log_path_text = self.log_filepath_input.text()
        if log_path_text and os.path.isdir(log_path_text):
            folder_name = os.path.basename(os.path.normpath(log_path_text))
            default_filename = f"{folder_name}_processed_logs.csv"
        else:
            default_filename = "processed_logs.csv" # Generic name if no log folder selected

        file, _ = QFileDialog.getSaveFileName(
            self, "Save CSV File", default_filename, "CSV Files (*.csv)"
        )
        if file:
            # Ensure the file has a .csv extension
            if not file.endswith(".csv"):
                file += ".csv"
            self.csv_result_input.setText(file)


    def browse_csv_sum(self) -> None:
        file, _ = QFileDialog.getOpenFileName(
            self, "Open CSV File", "", "CSV Files (*.csv)"
        )
        if file:
            self.csv_sum_filepath_input.setText(file)
            # self.summary_output_text.clear() # Removed as summary_output_text is removed
            # self.show_more_statistics.setHidden(True) # Removed show_more_statistics

    # START ======= Main Methods of Program ======== START #

    def export_to_excel_triggered(self) -> None:
        csv_file_path = self.csv_sum_filepath_input.text()
        if not csv_file_path:
            self.program_output_window.append("Please select a CSV file to export.")
            return
        
        if not os.path.exists(csv_file_path):
            self.program_output_window.append(f"Error: CSV file not found at '{csv_file_path}'.")
            return

        default_name = (
            f"{os.path.basename(csv_file_path).split('.')[0]}_excel_export.xlsx"
        )
        file_path, _ = QFileDialog.getSaveFileName(
            self, "Save Excel File", default_name, "Excel Files (*.xlsx)"
        )

        if file_path:
            # Ensure the file has a .xlsx extension
            if not file_path.endswith(".xlsx"):
                file_path += ".xlsx"
            self.start_export_to_excel(csv_file_path, file_path)
        else:
            self.program_output_window.append("Excel export cancelled by user.")

    # Worker Thread for exporting CSV files to Excel - To combat GUI freezes
    def start_export_to_excel(self, csv_file_path:str, excel_file_path:str) -> None:
        self.worker = GenericWorker("export_excel",csv_file_path, excel_file_path)
        self.worker.output_window.connect(self.write_to_output_window)
        self.worker.messagebox_info.connect(self.messagebox_popup_info)
        self.worker.messagebox_warn.connect(self.messagebox_popup_warn)
        self.worker.messagebox_crit.connect(self.messagebox_popup_crit)
        self.worker.output_window_clear.connect(self.clear_output_window)
        self.worker.start()
            
    #  Main method to search log files and write data to csv file
    def start_processing(self) -> None:
        log_filepath = self.log_filepath_input.text().strip()
        output_csv = self.csv_result_input.text()

        if not log_filepath:
            self.program_output_window.append("Please select a log files folder or a log file.")
            return
        if not output_csv:
            self.program_output_window.append("Please specify an output CSV file location.")
            return
        
        # Check if the output CSV path is valid
        output_dir = os.path.dirname(output_csv)
        if output_dir and not os.path.isdir(output_dir):
            try:
                os.makedirs(output_dir)
            except OSError as e:
                self.program_output_window.append(f"Error: Could not create output directory '{output_dir}'. {e}")
                return

        # Determine if log_filepath is a file or a directory
        files_to_process = []
        if os.path.isfile(log_filepath):
            if log_filepath.endswith("_message.log"):
                files_to_process = [log_filepath]
            else:
                self.program_output_window.append("Selected file is not a '_message.log' file. Please select a valid log file or a directory containing them.")
                return
        elif os.path.isdir(log_filepath):
            files_to_process = [
                os.path.join(log_filepath, f)
                for f in os.listdir(log_filepath)
                if f.endswith("_message.log") and os.path.isfile(os.path.join(log_filepath, f))
            ]
        
        if not files_to_process:
            self.program_output_window.append("No '_message.log' files found in the selected location.")
            return

        self.program_output_window.append("Starting to process log files... please wait.")
        self.start_button.setEnabled(False)
        self.cancel_button.setEnabled(True)
        self.progress_bar.setValue(0)
        self.worker = GenericWorker("write_log_data_to_csv", log_filepath, output_csv)
        self.worker.output_window.connect(self.update_progress)
        self.worker.status.connect(self.update_status)
        self.worker.finished.connect(self.processing_finished)
        self.worker.progress_value.connect(self.update_progress_bar)
        self.worker.start()

    # New method to start statistics generation and export
    def start_statistics_generation(self) -> None:
        csv_path = self.csv_sum_filepath_input.text()
        if not csv_path:
            self.program_output_window.append("Please select a CSV file for statistics generation.")
            return
        
        if not os.path.exists(csv_path):
            self.program_output_window.append(f"Error: CSV file not found at '{csv_path}'.")
            return

        self.program_output_window.append("Starting statistics generation and export... please wait.")
        self.start_button.setEnabled(False) # Disable start processing button during this task
        self.cancel_button.setEnabled(True)
        self.progress_bar.setValue(0)

        self.worker = GenericWorker("generate_statistics", csv_path)
        self.worker.output_window.connect(self.write_to_output_window)
        self.worker.messagebox_info.connect(self.messagebox_popup_info)
        self.worker.messagebox_warn.connect(self.messagebox_popup_warn)
        self.worker.messagebox_crit.connect(self.messagebox_popup_crit)
        self.worker.output_window_clear.connect(self.clear_output_window)
        self.worker.statistics_ready.connect(self.export_summary_statistic_to_excel) # Connect to the export method
        self.worker.finished.connect(self.processing_finished) # Use general finished signal
        self.worker.start()

    def export_summary_statistic_to_excel(self, statistics_dataframes: dict) -> None:
        # csv_path is not a member of self in LogSearcherGUI, it needs to be passed or re-obtained
        # For naming the output file, we'll use the name of the input CSV for consistency
        csv_path = self.csv_sum_filepath_input.text() # Re-obtain path for naming

        if not statistics_dataframes:
            QMessageBox.warning(self, "Export Failed", "No statistics data to export.")
            return

        try:
            file_path, _ = QFileDialog.getSaveFileName(
                self,
                "Save Statistics Excel File",
                f"{os.path.basename(csv_path).split('.')[0]}_statistics_{datetime.datetime.now().strftime('%Y-%m-%d')}.xlsx",
                "Excel Files (*.xlsx)",
            )
            if not file_path:
                QMessageBox.information(self, "Export Cancelled", "Statistics export cancelled by user.")
                return  # User canceled

            with pd.ExcelWriter(file_path, engine="xlsxwriter") as writer:
                for sheet_name, df_export in statistics_dataframes.items():
                    # Ensure all dataframes are actually created before trying to write
                    if not df_export.empty: # Check if the dataframe is not empty
                        df_export.to_excel(writer, sheet_name=sheet_name, index=False)

            QMessageBox.information(
                self,
                "Export Successful",
                f"Exported statistics to:\n{file_path}",
            )
            self.program_output_window.append(f"Statistics exported to: {file_path}")

        except Exception as e:
            QMessageBox.critical(self, "Export Failed", f"An error occurred during statistics export: {e}")
            self.program_output_window.append(f"Error exporting statistics: {e}")

    def open_logs_folder(self, path:str) -> None:
        if os.path.exists(path):
            self.log_filepath_input.setText(path)
            self.add_recent_folder(path)
            self.print_total_log_files(path)
        else:
            QMessageBox.warning(self, "Invalid Path", f"The path '{path}' does not exist or is not accessible.")
            self.status_bar.clearMessage() # Clear status bar if path is invalid


    def add_recent_folder(self, folder:str) -> None:
        if folder not in self.recent_folders:
            self.recent_folders.insert(0, folder)
            if len(self.recent_folders) > 5:
                self.recent_folders.pop()

            self.update_recent_menu()


    def update_recent_menu(self):
        self.recent_menu.clear()
        for f in self.recent_folders:
            action = QAction(f, self)
            action.triggered.connect(lambda checked, p=f: self.open_logs_folder(p))
            self.recent_menu.addAction(action)

    def clear_all_output(self) -> None:
        self.program_output_window.clear()
        # self.summary_output_text.clear() # Removed
        self.progress_bar.setValue(0)
        self.status_bar.clearMessage()
        # self.show_more_statistics.setHidden(True) # Removed show_more_statistics


    def show_about(self) -> None:
        about_text = """
<p style='font-weight:bold; font-size:16px;'>Lobster Message Log Searcher</p>
<p>Version 1.0.1</p>
<p>This tool searches for and processes <b>_message.log</b> files. It extracts relevant information and exports it to a CSV file. You can then analyze the data and generate statistics.</p>
<br>
<p style='font-weight:bold;'>Output CSV Headers:</p>
<ul>
    <li><b>Time:</b> Timestamp of the log entry.</li>
    <li><b>Job ID:</b> The ID of the job.</li>
    <li><b>Profile Name:</b> The profile name associated with the log entry.</li>
    <li><b>Filename:</b> The filename mentioned in the log.</li>
    <li><b>Filesize in Bytes:</b> The size of the file in bytes.</li>
</ul>
<br>
<p style='font-weight:bold;'>Features:</p>
<ul>
    <li>Efficient log file processing to CSV.</li>
    <li>Export processed data and statistics to Excel.</li>
    <li>Maintains a history of recent log folders.</li>
    <li>User-friendly graphical interface.</li>
</ul>
<br>
<p>Developed by: Your Name/Organization (if applicable)</p>
<p>Date: June 2025</p>
"""
        # Using setHtml for richer formatting in QTextEdit
        self.program_output_window.setHtml(about_text)

    # Clear the recent folders from QSettings and the "Recent Menu" Menubar
    def clear_recent_folders(self) -> None:
        reply = QMessageBox.question(self, "Clear Recent Folders",
                                     "Are you sure you want to clear the list of recent folders?",
                                     QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
        if reply == QMessageBox.Yes:
            # Remove the 'recent_folders' key from QSettings
            self.settings.remove("recent_folders")
            # Clear the in-memory list of recent folders
            self.recent_folders = []
            # Update the Recent Folders menu in the GUI
            self.update_recent_menu()
            self.status_bar.showMessage("Recent folders cleared.", 3000)

    # Program close event trigger
    def closeEvent(self, event: QCloseEvent) -> None:
        # Save geometry on close
        geometry = self.saveGeometry()
        self.settings.setValue("app_geometry", geometry)
        # Save recent folders
        self.settings.setValue("recent_folders", self.recent_folders)
        
        # Ensure worker thread is terminated if active
        if hasattr(self, 'worker') and self.worker.isRunning():
            self.worker.cancel() # Request cancellation
            self.worker.wait(2000) # Give it some time to finish
            if self.worker.isRunning():
                self.worker.terminate() # Force terminate if it doesn't respond
                self.worker.wait()
                QMessageBox.warning(self, "Process Terminated", "Log processing was forcibly terminated.")

        super().closeEvent(event) # Call the base class method


    # Prints the total log files found in the statusbar
    def print_total_log_files(self, filepath:str) -> None:
        try:
            if os.path.isfile(filepath):
                if filepath.endswith("_message.log"):
                    self.status_bar.showMessage(f"Selected single log file: {os.path.basename(filepath)}")
                else:
                    self.status_bar.showMessage(f"Selected file '{os.path.basename(filepath)}' is not a '_message.log' file.")
            elif os.path.isdir(filepath):
                files = [f for f in os.listdir(filepath) if f.endswith("_message.log") and os.path.isfile(os.path.join(filepath, f))]
                self.status_bar.showMessage(f"Total '_message.log' files found: {len(files)}")
            else:
                self.status_bar.clearMessage()
        except (TypeError, FileNotFoundError, PermissionError) as e:
            self.status_bar.showMessage(f"Error accessing path: {e}", 5000)
        except Exception as e:
            self.status_bar.showMessage(f"An unexpected error occurred: {e}", 5000)


    # ====== Slots for the Signals ====== #
    
    def write_to_output_window(self, message):
        self.program_output_window.append(message)
        
    def clear_output_window(self):
        self.program_output_window.clear()

    def messagebox_popup_info(self, title, message):
        QMessageBox.information(self, title, message)

    def messagebox_popup_warn(self, title, message):
        QMessageBox.warning(self, title, message)

    def messagebox_popup_crit(self, title, message):
        QMessageBox.critical(self, title, message)
        
    def cancel_processing(self):
        if hasattr(self, "worker") and self.worker.isRunning():
            self.worker.cancel_requested.emit()
            self.cancel_button.setEnabled(False)
            self.start_button.setEnabled(True)
            self.progress_bar.setValue(0)
            self.program_output_window.setText("Canceling operation, please wait...")
            self.status_bar.showMessage("Cancellation requested...", 0)
        else:
            self.program_output_window.append("No active processing to cancel.")


    def update_progress(self, message):
        self.program_output_window.append(message)
        self.program_output_window.ensureCursorVisible()

    def update_status(self, message):
        self.status_bar.showMessage(message)

    def update_progress_bar(self, value):
        self.progress_bar.setValue(value)

    def processing_finished(self, message):
        self.program_output_window.append(message)
        self.status_bar.showMessage("Processing complete!", 5000) # Clear after showing
        self.start_button.setEnabled(True)
        self.cancel_button.setEnabled(False)
        self.progress_bar.setValue(100)

    # ====== END Slots for the Signals END ====== #


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = LogSearcherGUI()
    window.show()
    sys.exit(app.exec())
