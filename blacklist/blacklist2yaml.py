import csv
import re
import os

# check if it's csv file
def is_csv(file_path):
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            sample = file.read(1024)  # read the sample contents
            return csv.Sniffer().sniff(sample)  # check the file extension
    except Exception:
        return False  

# check the barcode column and remove special character and duplicated rows
def read_barcodes_from_csv(file_path):
    barcodes = set()
    with open(file_path, "r", encoding="utf-8") as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            if "barcode" in row:
                barcode = row["barcode"]
                cleaned_barcode = re.sub(r'\W+', '', barcode) 
                barcodes.add(cleaned_barcode)
    return sorted(barcodes)

def main(input_file):
    # check if it's csv file
    if not input_file.endswith(".csv") and not is_csv(input_file):
        raise ValueError("Couldn't support this type of file. Please use csv file.")

    barcodes = read_barcodes_from_csv(input_file)

    with open("blacklist.yaml", "w", encoding="utf-8") as yf:
        yf.write("items:\n")
        for barcode in barcodes:
            yf.write(f"- barcode: {barcode}\n")

if __name__ == "__main__":
    input_file = "blacklist.csv" 
    main(input_file)
