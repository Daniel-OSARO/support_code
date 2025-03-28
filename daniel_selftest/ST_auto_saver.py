import os
import datetime

def get_today_date():
    """Returns the current date in MMDD format."""
    return datetime.datetime.now().strftime("%m%d")

def run_scp_command(cell, file_type):
    """
    Runs the SCP command for the specified cell and file type.
    :param cell: The cell number (1-7)
    :param file_type: The type of file ('combined' or 'summarized')
    """
    today_date = get_today_date()
    if file_type == "combined":
        command = f"scp 'admin@192.168.111.1{cell}:~/script/combined_ST_results.txt' ~/Documents/C{cell}ST{today_date}"
    elif file_type == "summarized":
        command = f"scp 'admin@192.168.111.1{cell}:~/script/summarized_results.txt' ~/Documents/summary_C{cell}ST{today_date}"
    else:
        raise ValueError("Invalid file type specified.")
    os.system(command)

def merge_summarized_files(selected_cells):
    """
    Merges the 'summary' files from ~/Documents into one file named 'integrated_summary_{today_date}.txt'.
    Cleans up the individual summary files after merging.
    :param selected_cells: List of selected cell numbers (used to identify files to merge).
    """
    today_date = get_today_date()
    summary_files = [f"~/Documents/summary_C{cell}ST{today_date}" for cell in selected_cells]
    
    # Properly format the file name
    if selected_cells == [0]:  # If all cells are selected
        integrated_file_path = f"~/Documents/integrated_summary_AllCells_{today_date}.txt"
    else:
        cell_str = ''.join(map(str, selected_cells))  # Join cell numbers like '1', '7'
        integrated_file_path = f"~/Documents/integrated_summary_Cell{cell_str}_{today_date}.txt"

    # Expand user paths
    summary_files = [os.path.expanduser(file) for file in summary_files]
    integrated_file_path = os.path.expanduser(integrated_file_path)

    # Merge files
    with open(integrated_file_path, "w") as outfile:
        for file in summary_files:
            if os.path.exists(file):
                with open(file, "r") as infile:
                    outfile.write(infile.read())
                os.remove(file)  # Remove the file after merging
            else:
                print(f"Warning: {file} does not exist and will not be included.")

    print(f"Merged summary files saved to '{integrated_file_path}'.")
    return integrated_file_path  # Return the path to the merged file



def main():
    print("\nThis script will automatically scp the ST results and summarize\n")
    selected_cells = input("Which cell do you want to get the ST results? (e.g., 1457). If you want all, type 0 : ")
    today_date = get_today_date()

    if selected_cells == "0":
        cells_to_process = range(1, 8)  # All cells (1 to 7)
    else:
        cells_to_process = [int(digit) for digit in selected_cells if digit.isdigit()]

    # Run SCP commands for each selected cell
    for cell in cells_to_process:
        run_scp_command(cell, "combined")
        run_scp_command(cell, "summarized")

    # Merge summarized results
    integrated_file_path = merge_summarized_files(cells_to_process)

    print("\n****************************************\n** Please check your Document folder! **\n****************************************\n")

if __name__ == "__main__":
    main()