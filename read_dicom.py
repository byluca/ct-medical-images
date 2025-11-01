# read_dicom.py
import os
from etl_utils import dicom_to_jpeg  # Import only the new function

# --- CONFIG ---
DATA_DIR = "data/dicom_dir"
JPEG_OUTPUT_DIR = "data/jpeg_images"  # Folder for output images
# ----------------

try:
    all_files = os.listdir(DATA_DIR)
    dicom_files = [f for f in all_files if f.endswith('.dcm')]
    
    if not dicom_files:
        print(f" Error: No .dcm files found in '{DATA_DIR}'.")
    else:
        # Take the first file for testing
        file_path = os.path.join(DATA_DIR, dicom_files[0])
        print(f"---  Converting file: {file_path} ---")
        
        # --- TEST CONVERSION ---
        jpeg_path = dicom_to_jpeg(file_path, JPEG_OUTPUT_DIR)
        
        if jpeg_path:
            print(f"\n---  Success! ---")
            print(f"New JPEG image saved at: {jpeg_path}")
            print(f"\nCheck the '{JPEG_OUTPUT_DIR}' folder in your project.")
        else:
            print(f"\n---  Failed ---")
            print(f"Could not convert {file_path}")

except FileNotFoundError:
    print(f" Error: Directory not found. Check if DATA_DIR is correct.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
