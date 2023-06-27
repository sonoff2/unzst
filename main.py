from csvbuilder import CSVBuilder
from datamanagement import FileManager

if __name__ == "__main__":
    # Parse the config file (not shown here).
    config = parse_config("config.json")

    # Initialize the file manager.
    file_manager = FileManager(config)

    # Split the zst file.
    file_manager.split_zst_file()

    # Initialize the csv builder.
    csv_builder = CSVBuilder(config, file_manager)

    # Process the gz files.
    csv_builder.process_files()

    print(f"Number of error lines: {file_manager.error_lines}")