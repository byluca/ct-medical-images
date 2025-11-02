# main_etl.py (CORRECT and IDEMPOTENT)

import os
import pydicom
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import datetime
from glob import glob

# Import all helper functions
from etl_utils import (
    format_age,
    surrogate_key,
    get_or_create,
    normalize_contrast_agent,
    normalize_pixel_spacing,
    dicom_to_jpeg
)

# --- Helper function ---
def get_safe_value(dcm_dataset, tag, default=None):
    """Safely get a DICOM tag value, return default if missing."""
    data_element = dcm_dataset.get(tag)
    if data_element is None:
        return default
    return data_element.value

# --- 1. CONFIGURATION ---
CONNECTION_STRING = "mongodb://localhost:27017/"
try:
    # Attempt MongoDB connection
    CLIENT = MongoClient(CONNECTION_STRING, serverSelectionTimeoutMS=5000)
    CLIENT.admin.command('ping')
except ConnectionFailure as e:
    print(f"❌ MongoDB Connection Error: {e}")
    print("Ensure MongoDB is running on port 27017.")
    exit()
except Exception as e:
    print(f"An unexpected error occurred during connection: {e}")
    exit()

# Define Database and Collections
DB = CLIENT["dicom_dw"]

dim_patient_coll = DB["dim_patient"]
dim_station_coll = DB["dim_station"]
dim_protocol_coll = DB["dim_protocol"]
dim_image_coll = DB["dim_image"]
dim_date_coll = DB["dim_date"]
fact_study_coll = DB["fact_study"]

DATA_DIR = "data/dicom_dir"
JPEG_OUTPUT_DIR = "data/jpeg_images"

# File discovery
DICOM_FILES = glob(os.path.join(DATA_DIR, "*.dcm"))
total_files = len(DICOM_FILES)
print(f"--- 🚀 Starting ETL Pipeline ---")
print(f"Found {total_files} DICOM files to process.")

# --- 2. ETL LOOP ---
for i, file_path in enumerate(DICOM_FILES):
    # Print progress status
    print(f"  -> Processing file {i+1}/{total_files}: {os.path.basename(file_path)}...", end='\r')
    
    try:
        # EXTRACT: Read DICOM file
        dcm = pydicom.dcmread(file_path)

        # --- TRANSFORM & LOAD (DIMENSIONS) ---
        
        # 1. Patient dimension
        patient_values = {
            "patient_id": get_safe_value(dcm, (0x0010, 0x0020), "Unknown"),
            "sex": get_safe_value(dcm, (0x0010, 0x0040), "Unknown"),
            "age": format_age(get_safe_value(dcm, (0x0010, 0x1010)))
        }
        # Get or create dimension record
        patient_sk = get_or_create(dim_patient_coll, patient_values, "patient_sk")

        # 2. Station dimension
        station_values = {
            "manufacturer": get_safe_value(dcm, (0x0008, 0x0070), "Unknown"),
            "model": get_safe_value(dcm, (0x0008, 0x1090), "Unknown")
        }
        station_sk = get_or_create(dim_station_coll, station_values, "station_sk")

        # 3. Protocol dimension
        protocol_values = {
            "body_part": get_safe_value(dcm, (0x0018, 0x0015), "Unknown"),
            "contrast_agent": normalize_contrast_agent(get_safe_value(dcm, (0x0018, 0x0010))),
            "patient_position": get_safe_value(dcm, (0x0018, 0x5100), "Unknown")
        }
        protocol_sk = get_or_create(dim_protocol_coll, protocol_values, "protocol_sk")

        # 4. Image dimension (Handling pixel spacing list structure)
        raw_spacing = get_safe_value(dcm, (0x0028, 0x0030), [None, None])
        spacing_x = raw_spacing[1] if isinstance(raw_spacing, list) and len(raw_spacing) > 1 else raw_spacing
        spacing_y = raw_spacing[0] if isinstance(raw_spacing, list) else None
        
        image_values = {
            "rows": get_safe_value(dcm, (0x0028, 0x0010)),
            "columns": get_safe_value(dcm, (0x0028, 0x0011)),
            "pixel_spacing_x": normalize_pixel_spacing(spacing_x),
            "pixel_spacing_y": normalize_pixel_spacing(spacing_y),
            "slice_thickness": get_safe_value(dcm, (0x0018, 0x0050)),
            "photometric_interp": get_safe_value(dcm, (0x0028, 0x0004), "Unknown")
        }
        image_sk = get_or_create(dim_image_coll, image_values, "image_sk")

        # 5. Date dimension (Handling date parsing)
        date_str = get_safe_value(dcm, (0x0008, 0x0022))  # Acquisition Date
        try:
            dt = datetime.datetime.strptime(str(date_str), '%Y%m%d')
            date_values = {"year": dt.year, "month": dt.month, "full_date": dt}
        except (ValueError, TypeError):
            date_values = {"year": None, "month": None, "full_date": None}
        
        date_sk = get_or_create(dim_date_coll, date_values, "date_sk")

        # --- TRANSFORM & LOAD (FACT TABLE) ---
        # Convert DICOM to JPEG
        jpeg_path = dicom_to_jpeg(file_path, JPEG_OUTPUT_DIR)
        
        # Create Fact document
        fact_study = {
            "patient_sk": patient_sk,
            "station_sk": station_sk,
            "protocol_sk": protocol_sk,
            "image_sk": image_sk,
            "date_sk": date_sk,
            "exposure_time": get_safe_value(dcm, (0x0018, 0x1150)),
            "tube_current": get_safe_value(dcm, (0x0018, 0x1151)),
            "file_path": jpeg_path # Used as uniqueness key for fact table
        }
        
        # --- IDEMPOTENCY CORRECTION ---
        if jpeg_path:
            # 1. Check if a study with this file_path already exists.
            existing_fact = fact_study_coll.find_one({"file_path": jpeg_path})
            
            if existing_fact is None:
                # 2. If it does not exist, insert the new study.
                fact_study_coll.insert_one(fact_study)
            # else:
                # Study already exists, skip insertion (idempotence achieved).
        else:
            print(f"\n  ⚠️ Skipped {os.path.basename(file_path)}. JPEG conversion failed.")

    except Exception as e:
        # File-level resilience: log error and continue to the next file
        print(f"\n  ❌ Error processing {os.path.basename(file_path)}: {e}. File skipped.")

print("\n" + "="*50)
print("--- ✅ ETL Pipeline Completed ---")
print(f"Database '{DB.name}' loaded.")
print("Check results in MongoDB Compass.")
print("="*50)