# read_dicom.py
import os
from etl_utils import dicom_to_jpeg # Importiamo solo la nuova funzione

# --- CONFIG ---
DATA_DIR = "data/dicom_dir"
JPEG_OUTPUT_DIR = "data/jpeg_images" # Una nuova cartella per le nostre immagini
# ----------------

try:
    all_files = os.listdir(DATA_DIR)
    dicom_files = [f for f in all_files if f.endswith('.dcm')]
    
    if not dicom_files:
        print(f"❌ Error: No .dcm files found in '{DATA_DIR}'.")
    else:
        # Prendi il primo file per il test
        file_path = os.path.join(DATA_DIR, dicom_files[0])
        print(f"--- 🖼️ Converting file: {file_path} ---")
        
        # --- TESTIAMO LA CONVERSIONE ---
        jpeg_path = dicom_to_jpeg(file_path, JPEG_OUTPUT_DIR)
        
        if jpeg_path:
            print(f"\n--- ✅ Success! ---")
            print(f"New JPEG image saved at: {jpeg_path}")
            print(f"\nControlla la cartella '{JPEG_OUTPUT_DIR}' nel tuo progetto.")
        else:
            print(f"\n--- ❌ Failed ---")
            print(f"Could not convert {file_path}")

except FileNotFoundError:
    print(f"❌ Error: Directory not found. Check if DATA_DIR is correct.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")