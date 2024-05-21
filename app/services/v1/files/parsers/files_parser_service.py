import math
import random
import uuid
from io import BytesIO
from typing import List

import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException, APIRouter
from fastapi.encoders import jsonable_encoder
from starlette.responses import JSONResponse
import logging
import json
import numpy as np
from datetime import datetime, timedelta

# Initialize logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Initialize FastAPI app and router
app = FastAPI()
router = APIRouter()

# Load the template from the specified file in the root directory
template_file_path = 'app/storage/extract_templates/poc_template.json'
with open(template_file_path, 'r') as template_file:
    template = json.load(template_file)


def dynamic_apply_template(df, template_actions):
    # Initially, create a set to keep track of all columns to retain
    columns_to_retain = set()

    for action in template_actions:
        column_names = action["column_name"]
        field_name = action["field_name"]

        # Ensure the field_name is included in the columns to retain
        columns_to_retain.add(field_name)

        if action["action"] == "split":
            # Assuming each column in column_names will have the same split
            for column_name in column_names:
                # Create the new field with an empty dict to populate later
                df[field_name] = [{} for _ in range(len(df))]

                split_info = action["split_into"]
                for part_key, part_value in split_info.items():
                    # Split and populate the new dictionary field
                    df[field_name] = df.apply(
                        lambda row: {**row[field_name],
                                     part_key: float(row[column_name].split(action["separator"])[0].strip())
                                     if part_key == 'lat'
                                     else float(row[column_name].split(action["separator"])[1].strip())},
                        axis=1
                    )
        elif action["action"] == "*":
            # Check for the 'format' key
            if 'format' in action:
                # If 'clean_spaces' option is selected
                if action['format'] == 'clean_spaces':
                    for column_name in column_names:
                        if df[column_name].dtype == 'object':  # Check if the column contains string values
                            df[field_name] = df[column_name].str.replace(' ', '')  # Remove spaces
                        else:
                            df[field_name] = df[column_name]
                # If 'lower_case' option is selected
                elif action['format'] == 'lower_case':
                    for column_name in column_names:
                        if df[column_name].dtype == 'object':  # Check if the column contains string values
                            df[field_name] = df[column_name].str.lower()  # Convert to lower case
                        else:
                            df[field_name] = df[column_name]
            else:
                # No special formatting specified, simply copy the value without any changes
                for column_name in column_names:
                    if df[column_name].dtype == 'object':  # Check if the column contains string values
                        df[field_name] = df[column_name].str.replace(' ', '')  # Remove spaces
                    else:
                        df[field_name] = df[column_name]
        elif action["action"] == "date_range":
            array_info = action["array"]
            result = []

            for index, row in df.iterrows():
                date_values = {}
                from_date = None  # Initialize variable to hold the "from" date for comparison

                # Process each column specified in the action
                for column_name, part_key in zip(column_names, array_info.keys()):
                    # Extract the prefix to determine if the field is "from" or "to"
                    prefix = array_info[part_key]["prefix"]
                    date_str = row[column_name].strip()  # Strip whitespace from the date string

                    # Convert the date string to a datetime object
                    current_date = pd.to_datetime(date_str)

                    if prefix == "from":
                        from_date = current_date  # Save "from" date for later comparison
                        date_values[prefix] = current_date.strftime('%Y-%m-%dT%H:%M:%SZ')

                    elif prefix == "to":
                        # Ensure that we have a "from" date to compare against
                        if from_date is not None and current_date < from_date:
                            current_date += timedelta(days=1)  # Add a day if "to" date is before "from" date
                        date_values[prefix] = current_date.strftime('%Y-%m-%dT%H:%M:%SZ')
                    else:
                        # Handle any other date types that might be present
                        date_values[prefix] = current_date.strftime('%Y-%m-%dT%H:%M:%SZ')

                result.append(date_values)

            df[field_name] = pd.Series(result)  # Assign the processed date values back to the DataFrame

        elif action["action"] == "concat_uuid":
            # Concatenate required_vehicle_type with a UUID
            for column_name in column_names:
                df[field_name] = df.apply(
                    lambda row: f"order_{row[column_name]}_{uuid.uuid4().hex[:8]}",
                    axis=1
                )
        elif action["action"] == "minutes_to_seconds":
            for column_name in column_names:
                df[field_name] = df[column_name] * 60

    # Replace NaN values or empty strings with None
    df.replace({np.nan: None, '': None}, inplace=True)

    # Retain only the columns that were processed as per the template, plus any newly created columns
    df = df[list(columns_to_retain)]

    return df


def read_vehicle_types_from_file(file_path):
    with open(file_path, 'r') as file:
        vehicle_types = json.load(file)
    return vehicle_types


# this function will be deleted ....
def generate_nearby_location(lat, lng, max_distance_in_meters):
    """
    Generate a nearby location within a random distance up to a maximum distance in meters.
    """
    R = 6378.1  # Radius of the Earth in kilometers

    # Generate a random distance up to the maximum distance
    distance_in_meters = random.uniform(0, max_distance_in_meters)

    # Convert distance to kilometers
    distance_km = distance_in_meters / 1000

    # Randomize bearing between 0 and 360 degrees
    bearing = math.radians(random.randint(0, 360))

    lat1 = math.radians(lat)
    lon1 = math.radians(lng)

    lat2 = math.asin(math.sin(lat1) * math.cos(distance_km / R) +
                     math.cos(lat1) * math.sin(distance_km / R) * math.cos(bearing))
    lon2 = lon1 + math.atan2(math.sin(bearing) * math.sin(distance_km / R) * math.cos(lat1),
                             math.cos(distance_km / R) - math.sin(lat1) * math.sin(lat2))

    new_lat = math.degrees(lat2)
    new_lng = math.degrees(lon2)

    return new_lat, new_lng


def generate_vehicle_locations(shipments: List[dict], w: int) -> List[dict]:
    vehicle_locations = []
    # Extract pickup locations from shipments
    pickup_locations = list(set((shipment['pickup']['lat'], shipment['pickup']['lng']) for shipment in shipments))
    random.shuffle(pickup_locations)

    file_path = 'app/storage/profiles/vehicles/silal.json'
    vehicle_types = read_vehicle_types_from_file(file_path)

    # Create all vehicles without assigning locations
    vehicles = []
    for vehicle_type in vehicle_types:
        count = vehicle_type["demo_count"]
        on_demand = vehicle_type["demo_count"] * 3
        for _ in range(count):
            vehicle_uuid = uuid.uuid4().hex[:8]
            vehicle = {
                'type': vehicle_type["name"],
                'capacity': vehicle_type["capacity"],
                'label': f"{vehicle_type['name']}_{vehicle_uuid}",
                'display_name': f"vehicle_{vehicle_type['name']}_{vehicle_uuid}",
                'on_demand': False,
                'cost': 1
            }
            vehicles.append(vehicle)

        for _ in range(on_demand):
            vehicle_uuid = uuid.uuid4().hex[:8]
            vehicle = {
                'type': vehicle_type["name"],
                'capacity': vehicle_type["capacity"],
                'label': f"{vehicle_type['name']}_{vehicle_uuid}",
                'display_name': f"vehicle_{vehicle_type['name']}_{vehicle_uuid}",
                'on_demand': True,
                'cost': 1000
            }
            vehicles.append(vehicle)

    # Assign vehicles to pickup locations in a round-robin manner
    pickup_index = 0
    for vehicle in vehicles:
        nearby_lat, nearby_lng = generate_nearby_location(pickup_locations[pickup_index][0],
                                                          pickup_locations[pickup_index][1], w)
        vehicle['lat'] = nearby_lat
        vehicle['lng'] = nearby_lng
        vehicle_locations.append(vehicle)

        # Move to the next pickup location in a round-robin manner
        pickup_index = (pickup_index + 1) % len(pickup_locations)

    return vehicle_locations


@router.post("/api/v1/files/parse")
async def parse(file: UploadFile = File(...)):
    try:
        if file.content_type == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
            contents = await file.read()
            data = BytesIO(contents)

            # Specify the sheet names directly from the template
            sheet_names = list(template.keys())  # Assume template might specify multiple sheets
            dfs = pd.read_excel(data, sheet_name=sheet_names)

            processed_data = {}
            for sheet_name, df in dfs.items():
                if sheet_name in template:
                    df_processed = dynamic_apply_template(df, template[sheet_name])
                    processed_data[sheet_name] = df_processed.to_dict(orient='records')

            # If only interested in "Stock transfer & Delivery", directly return its data
            if "Stock transfer & Delivery" in processed_data:
                records = processed_data["Stock transfer & Delivery"]
                vehicles = generate_vehicle_locations(records,
                                                      150)
                records = apply_exclusive_customers(records, processed_data['Sheet2'])
                return {'records': records, 'vehicles': vehicles, 'exclusive_customers': processed_data['Sheet2']}
            else:
                return JSONResponse(content={"error": "Specified sheet not found in the file"}, status_code=404)
        else:
            raise HTTPException(status_code=400, detail="Unsupported file type")
    except Exception as e:
        logger.exception("An error occurred during file processing", exc_info=e)
        raise HTTPException(status_code=500, detail="Internal server error")


def apply_exclusive_customers(data, exclusive_customers):
    # Iterate over each element in data
    for element in data:
        # Check if the customer exists in the exclusive_customers array
        customer = element.get('customer')
        if customer in (customer_info.get('customer') for customer_info in exclusive_customers):
            element['exclusive'] = True  # Set flag to True if customer is exclusive
        else:
            element['exclusive'] = False  # Set flag to False if customer is not exclusive

    return data  # Return the modified data array
