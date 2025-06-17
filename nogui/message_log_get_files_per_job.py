import csv
import os
import re
from tqdm import tqdm
import pandas as pd
import datetime

"""
This is a standalone non GUI version of the log file processor.
It reads the *_message.log files and uses the regex pattern to match each line that contains the processed file, it's name and as well as the filesize in Bytes
The matched line's content is written to a CSV file and at the same time each filesize is converted from Bytes to MB for easier reading.
"""

class Worker():
    def __init__(self, task:str, *args, **kwargs):
        super().__init__()
        self.task = task  # A string to identify the task: "export_excel" or "search_logs" or "generate_statistics"
        self.args = args  # Arguments for the task
        self.kwargs = kwargs # Keyword arguments for the task
        self.pattern = re.compile(r"\b(\d{2}:\d{2}:\d{2})\b\s+NORMAL\s+Job:\s+(GENERAL|\d+)\s+\[(.*?)\].*?Start processing data of file '(.+?)'@.*?length=(\d+)")
        self.statistics_dataframes = {} # Initialize dictionary to hold dataframes

    def run(self):
        if self.task == "export_excel":
            self.export_csv_to_excel(*self.args, **self.kwargs)
        elif self.task == "write_log_data_to_csv":
            self.extract_and_write_to_csv(*self.args, **self.kwargs)
        elif self.task == "generate_statistics": # New task for statistics generation
            self.generate_and_emit_statistics(*self.args, **self.kwargs)
        else:
            print(f"Unknown task: {self.task}")


    def export_csv_to_excel(self, csv_file_path:str, excel_file_path:str) -> None:
        if not csv_file_path:
            print("Please select a CSV file to convert.")
            return
        else:
            print("Exporting CSV to Excel... please wait.")

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

                print(f"Successfully converted:\n{csv_file_path}\nto\n{excel_file_path}")
            else:
                print(f"The CSV file exceeds the maximum row limit 1048576 for Excel. Your file contains {csv_row_count} rows.\nFile conversion not possible.")
            

        except FileNotFoundError:
            print(f"The file '{csv_file_path}' was not found.")
        except pd.errors.EmptyDataError:
            print("The selected CSV file is empty.")

        except Exception as ex:
            message = f"An exception of type {type(ex).__name__} occurred. Arguments: {ex.args!r}"
            print(f"Error exporting CSV: {message}")


    def process_file(self, filepath:str, file_index:int, total_files:int, writer) -> int:
        
        total_lines = self.count_lines(filepath)
        if total_lines == 0:
            print(f"Skipping empty file: {filepath}")
            return 0

        filename_for_output = os.path.basename(filepath)
        print(f"Processing {filename_for_output} - {file_index}/{total_files} - (Filesize: {os.path.getsize(filepath)} bytes, {total_lines} lines)")

        # Extracting date from filename like 13_05_2025_message.log -> 13_05_2025
        filename_prefix = filename_for_output.split('_message.log')[0] if '_message.log' in filename_for_output else filename_for_output

        matches_in_file = 0
        
        if total_lines > 0:
            with open(filepath, "r", encoding="utf-8", errors="ignore") as log_file:
                for line in tqdm(log_file, total=total_lines, desc=f"Processing {filename_for_output}", unit="line"):
                    extracted_info = self.extract_info_from_line(line, filename_prefix)
                    if extracted_info:
                        writer.writerow(extracted_info)
                        matches_in_file += 1
            print(f">>> Finished processing {filename_for_output}.")
            return matches_in_file
        else:
            return 0


    def extract_and_write_to_csv(self, log_file_path:str, csv_file_output_path:str) -> str:
        try:
            """searches *message.log log files and exports found processed files data to csv

            Args:
                filepath (str): File path to the message.log file
                output_file_csv (str): File path of the csv result file

            Returns:
                str: File path of the csv result file
            """
            files = []
            if os.path.isfile(log_file_path):
                if log_file_path.endswith("_message.log"):
                    files = [log_file_path]
                else:
                    print("Selected file is not a '_message.log' file. Please select a valid log file or a directory containing them.")
                    return
            elif os.path.isdir(log_file_path):
                files = [
                    os.path.join(log_file_path, f)
                    for f in os.listdir(log_file_path)
                    if f.endswith("_message.log") and os.path.isfile(os.path.join(log_file_path, f))
                ]
                if not files:
                    print("No '_message.log' files found in the selected directory.")
                    return
            else:
                print("Invalid filepath. Please select a valid file or directory.")
                return

            if not os.path.dirname(csv_file_output_path):
                os.makedirs(os.path.dirname(csv_file_output_path), exist_ok=True)
        except FileNotFoundError:
            print("File not found error in extract_and_write_to_csv")

        total_matches = 0


        try:
            with open(
                csv_file_output_path, "w", newline="", encoding="utf-8"
            ) as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(
                    [
                        "Time",
                        "Job ID",
                        "Profile Name",
                        "Filename",
                        "Filesize in KB",
                    ]
                )

                for idx, file in enumerate(files, start=1):
                    matches_in_file = self.process_file(file, idx, len(files), writer)
                    total_matches += matches_in_file

                print(
                    f"\nData written to: {csv_file_output_path}\n"
                    f"Total Log Files Processed: {len(files)}\n"
                    f"Total Matches Found: {total_matches}"
                )
                
            return csv_file_output_path
            
        except Exception as e:
            print(f"An error occurred while writing to CSV: {e}")


    # Helper methods
    def count_lines(self, filepath:str) -> int:
        count = 0
        try:
            with open(filepath, "r", encoding="utf-8", errors="ignore") as file:
                for _ in file:
                    count += 1
        except Exception as e:
            print(f"Error counting lines in {filepath}: {e}")
            return 0
        return count


    def extract_info_from_line(self, line:str, date_prefix:str) -> list:
        match = self.pattern.search(line)
        if match:
            # Combining date prefix with the time extracted from the log line
            full_timestamp = f"{date_prefix} {match.group(1)}"
            file_size = round(int(match.group(5)) / 1000, 2)  # 2 decimal places --- if int(match.group(5)) > 1024 else int(match.group(5)) If size > 1024 convert to kb, else leave to bytes, problem = No dynamic CSV header generation
            return [full_timestamp, match.group(2), match.group(3), match.group(4), file_size]
        return None


    def generate_and_emit_statistics(self, csv_file_output_path:str) -> None:
        try:
            print("Generating profile size analysis... please wait.")
            self.create_profile_size_analysis(csv_file_output_path)
            print("Statistics generation complete.")
            self.export_summary_statistic_to_excel(self.statistics_dataframes, csv_file_output_path )
        except Exception as e:
            print(f"An error occurred during statistics generation: {e}")
        
        finally:
            # --- CRITICAL MEMORY CLEANUP ---
            if hasattr(self, 'statistics_dataframes'):
                # Explicitly delete the reference to the large DataFrame(s)
                # This makes them eligible for Python's garbage collection
                del self.statistics_dataframes
                self.statistics_dataframes = None

            # Other large dataframes that are specific to tasks
            # For example, if 'df_logs' or 'filtered_df' are instance variables:
            if hasattr(self, 'df_logs'):
                del self.df_logs
                self.df_logs = None
            if hasattr(self, 'filtered_df'):
                del self.filtered_df
                self.filtered_df = None

    def create_profile_size_analysis(self, csv_file_path:str) -> None:
        try:
            df = pd.read_csv(csv_file_path, usecols=["Filename", "Filesize in KB", "Profile Name"], engine="pyarrow")

            profile_size_summary = (
                df.groupby("Profile Name")
                .agg(
                    {
                        "Filesize in KB": ["mean", "std", "min", "max", "count"],
                        "Filename": lambda x: list(dict.fromkeys(filter(None, x)))[:2],
                    }
                )
                .reset_index()
            )
            profile_size_summary.columns = [
                "Profile Name",
                "Avg File Size (KB)",
                "Std File Size (KB)",
                "Min File Size (KB)",
                "Max File Size (KB)",
                "Count",
                "File Samples",
            ]
            profile_size_summary["Avg File Size (KB)"] = profile_size_summary["Avg File Size (KB)"].round(2)
            profile_size_summary["Std File Size (KB)"] = profile_size_summary["Std File Size (KB)"].round(2)
            profile_size_summary["File Samples"] = profile_size_summary["File Samples"].apply(lambda x: ", ".join(x) if x else "None")

            # Store for export
            self.statistics_dataframes["PerProfileStatistics"] = profile_size_summary

        except FileNotFoundError:
            print(f"CSV file '{csv_file_path}' for profile analysis not found.")
        except pd.errors.EmptyDataError:
            print("The selected CSV file for profile analysis is empty.")
        except KeyError:
            print("Column Missing", "Required column 'Filesize in KB' or 'Filename' or 'Profile Name' not found in CSV for profile analysis.")
        except Exception as e:
            print("Error creating profile size analysis", f"An exception occurred during profile size analysis: {e}")
        finally:
            del df
    
    def export_summary_statistic_to_excel(self, statistics_dataframes: dict, csv_file_path: str) -> None:
        csv_path = csv_file_path # Re-obtain path for naming

        if not statistics_dataframes:
            print("No statistics data to export.")
            return

        try:
            excel_output_file_name = f"{datetime.datetime.now().strftime('%Y.%m.%d')}_{os.path.basename(csv_path).split('.')[0]}_statistics.xlsx"
            file_path = os.path.join(os.path.dirname(csv_file_path), excel_output_file_name)
            
            if not file_path:
                return 
            
            with pd.ExcelWriter(file_path, engine="xlsxwriter") as writer:
                for sheet_name, df_export in statistics_dataframes.items():
                    if not df_export.empty:
                        df_export.to_excel(writer, sheet_name=sheet_name, index=False)

                        # Add Excel Table formatting
                        workbook  = writer.book
                        worksheet = writer.sheets[sheet_name]
                        (max_row, max_col) = df_export.shape

                        column_settings = [{"header": col} for col in df_export.columns]

                        worksheet.add_table(0, 0, max_row, max_col - 1, {
                            "columns": column_settings,
                            "style": "Table Style Medium 16",
                            "name": f"{sheet_name[:30]}",  # Optional: Table name (must be <=31 chars)
                            "autofilter": True  # Set True if you want Excel filter buttons
                        })

                        # Optional: Improve column width
                        worksheet.set_column(0, max_col - 1, 18)

            print(f"Exported statistics to: {file_path}")
            statistics_dataframes.clear()

        except Exception as e:
            print(f"An error occurred during statistics export: {e}")

if __name__ == "__main__":
    
    log_files_folder_path = r"C:\Users\ZaricJ\Downloads\ProdMessageLogs"
    csv_file = r"C:\Users\ZaricJ\Downloads\CSVResult\ProdMessageLogs.csv"

    worker_csv_export = Worker("write_log_data_to_csv")
    worker_csv_export.extract_and_write_to_csv(log_file_path=log_files_folder_path, csv_file_output_path=csv_file)
    worker_generate_statistics = Worker("generate_statistics")
    worker_generate_statistics.generate_and_emit_statistics(csv_file_output_path=csv_file)

