import sys
from waybackup_to_warc import process_csv_file
# TODO: Bind both parts of the program together in this main class, which should support argparse for input and output files and making it possible to 
# run both parts of the program in one go or seperate as needed..
def main():
    if len(sys.argv) < 3:
        print("Usage: python main.py <csv_file_path> <output_filename>")
        sys.exit(1)
    csv_file_path = sys.argv[1]
    output_filename = sys.argv[2]
    output_dir = 'output'  # Update with your desired output directory
    process_csv_file(csv_file_path, output_dir, output_filename)

if __name__ == "__main__":
    main()