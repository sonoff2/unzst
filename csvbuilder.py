import json
import gzip
import pandas as pd
import re

import exclude_functions

class CSVBuilder:
    def __init__(self, config, file_manager):
        self.config = config
        self.file_manager = file_manager

        # Load the column functions.
        self.column_functions = self.load_column_functions()

    def load_column_functions(self):
        column_functions = {}

        for column, func_name in self.config.get('exclude_funcs', {}).items():
            # Use getattr to get the function from the module.
            func = getattr(exclude_functions, func_name, None)

            # If the function exists, add it to the column functions.
            if func is not None:
                column_functions[column] = func

        return column_functions

    def lines(self, s, splitter='\n', regex=True):
        if not regex:
            splitter = re.escape(splitter)

        start = 0

        for m in re.finditer(splitter, s):
            begin, end = m.span()
            if begin != start:
                yield s[start:begin]
            start = end

        if s[start:]:
            yield s[start:]

    def line_to_dict(self, line):
        # Convert the line into a dictionary.
        data = json.loads(line)

        # Check for column exclusions.
        for column, func in self.column_functions.items():
            if column in data and func(data[column]):
                return None

        return data

    def process_gz_to_csv(self, gz_path, csv_path):
        # Process each gz file to csv.
        with gzip.open(gz_path, 'rb') as f:
            file_content = f.read().decode()
            useful_data = []

            for line in self.lines(file_content, regex=False):
                if line:
                    try:
                        line_dict = self.line_to_dict(line)
                        if line_dict is not None:
                            useful_data.append(line_dict)
                    except Exception as e:
                        print(f"Error processing line: {e}")
                        self.file_manager.error_lines += 1

            # Convert the data into a DataFrame.
            df = pd.DataFrame(useful_data)

            # Save the DataFrame to a CSV file.
            df.to_csv(csv_path, index=None)

    def process_files(self):
        # Get the list of gz files.
        gz_files = self.file_manager.get_gz_chunk_files()

        # Process each gz file.
        for gz_file in gz_files:
            csv_path = self.file_manager.get_csv_path(gz_file)
            self.process_gz_to_csv(gz_file, csv_path)

        # Merge the csv files into one.
        self.file_manager.merge_csv_files()

        # Cleanup the temporary files.
        self.file_manager.cleanup()

