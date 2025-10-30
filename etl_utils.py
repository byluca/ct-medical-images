# etl_utils.py
import hashlib
import json
from pymongo.collection import Collection
import numpy as np
import pydicom      
from PIL import Image 
import os           


def format_age(age_str):
    """
    Transforms a DICOM age string like '061Y' into an integer (61).
    Handles missing or malformed data safely. 
    """
    if not age_str:
        return None
    
    try:
        age_val = int(age_str.rstrip('Y'))
        return age_val
    except ValueError:
        return None 
    except Exception as e:
        print(f"Error formatting age: {e}")
        return None


def surrogate_key(values_dict: dict) -> str:
    """
    Creates a stable MD5 hash from a dictionary's values.
    """
    # Sort the dictionary by key to ensure consistent hash
    sorted_dict = dict(sorted(values_dict.items()))
    
    # Convert dict to a JSON string
    dict_string = json.dumps(sorted_dict, default=str)
    
    # Create MD5 hash
    hash_object = hashlib.md5(dict_string.encode())
    return hash_object.hexdigest()


def get_or_create(collection: Collection, values: dict, pk_name: str) -> str:
    """
    Finds a record or creates it if it doesn't exist.
    Returns the surrogate key. 
    """
    # Create the surrogate key from the values
    sk = surrogate_key(values)
    
    # Check if a document with this surrogate key already exists
    query = {pk_name: sk}
    document = collection.find_one(query)
    
    if document:
        # Found it, just return the key
        return sk
    else:
        # Not found, create a new document
        new_doc = values.copy()
        new_doc[pk_name] = sk  # Add the surrogate key as the PK 
        collection.insert_one(new_doc)
        return sk
    

    



def normalize_pixel_spacing(raw_value) -> float:
    """
    Rounds a numeric pixel spacing value to the nearest predefined bin. 
    The predefined bins are: 0.6, 0.65, 0.7, 0.75, 0.8.
    """
    # Convert the raw value (which may be a string) to float
    try:
        val = float(raw_value)
    except (ValueError, TypeError):
        # If it's not a valid number, return None as default
        return None 

    bins = np.array([0.6, 0.65, 0.7, 0.75, 0.8])
    
    # Find the index of the closest bin
    nearest_bin_index = np.argmin(np.abs(bins - val))
    
    # Return the closest bin value
    return bins[nearest_bin_index]



def normalize_contrast_agent(val: str) -> str:
    """
    Standardizes the DICOM contrast agent field. 
    Replaces missing or empty values with 'No contrast agent'.
    """
    # Check if value is None, empty, or too short
    if val is None or not str(val).strip() or len(str(val).strip()) <= 1:
        return "No contrast agent"
    else:
        # Return cleaned value (remove spaces)
        return str(val).strip()


def dicom_to_jpeg(input_path: str, output_dir: str, size: tuple = (256, 256)) -> str:
    """Convert a DICOM file to a resized JPEG image."""
    
    # Make sure output folder exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Read DICOM file
    try:
        dcm = pydicom.dcmread(input_path)
    except Exception as e:
        print(f"Error reading DICOM file {input_path}: {e}")
        return None

    # Get and normalize pixel data (0–255)
    try:
        pixels = dcm.pixel_array.astype(float)

        # Apply intercept and slope if present
        if 'RescaleIntercept' in dcm:
            pixels = pixels * dcm.RescaleSlope + dcm.RescaleIntercept

        # Normalize to 0–255
        pixels = (pixels - np.min(pixels)) / (np.max(pixels) - np.min(pixels) + 1e-9)
        pixels = (pixels * 255.0).astype(np.uint8)

    except Exception as e:
        print(f"Error processing pixels for {input_path}: {e}")
        return None

    # Skip multi-frame images
    if pixels.ndim > 2:
        print(f"Skipping multi-frame DICOM {input_path}")
        return None

    # Create and resize image
    pil_image = Image.fromarray(pixels)
    if pil_image.mode != 'L':
        pil_image = pil_image.convert('L')
    pil_image = pil_image.resize(size)

    # Save JPEG
    base_name = os.path.basename(input_path)
    name_no_ext = os.path.splitext(base_name)[0]
    output_path = os.path.join(output_dir, f"{name_no_ext}.jpeg")
    pil_image.save(output_path)

    return output_path
