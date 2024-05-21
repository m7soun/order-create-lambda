import json
import logging
import os
import time

import requests
from datetime import datetime, timedelta, date

from google.cloud import optimization_v1
from concurrent.futures import ThreadPoolExecutor, as_completed

from functools import lru_cache

from app.models.geo.vrp.cfr.vehicle import Vehicle
from app.models.geo.vrp.cfr.shipment import Shipment


class CFR:

    def __init__(self, template_path, data):
        self.template_path = template_path
        self.data = data

    def get_template_content(self):
        with open(self.template_path, 'r') as file:
            template_content = json.load(file)
        return template_content

    def extract_models(self):
        models = []
        if "model" in self.get_template_content():
            model_data = self.get_template_content()["model"]
            for key, value in model_data.items():
                if isinstance(value, list):
                    models.append(key)
        return models

    def model_parse(self, data):
        project_id = os.getenv("PROJECT_ID")

        # Wrap the payloads within a 'model' object
        model_payload = {"parent": os.getenv("PROJECT_ID"), "model": data}

        # Return the model_payload
        return model_payload

    def prepare_exclusive(self):
        for record in self.data["records"]:
            if record["exclusive"]:
                record["shipment_type"] = record["customer"]
            else:
                record["shipment_type"] = "general"

    def create_incompatibilities(self):
        types = ["general"] + [customer["customer"] for customer in self.data["exclusive_customers"]]
        incompatibility_mode = "NOT_IN_SAME_VEHICLE_SIMULTANEOUSLY"

        return {
            "types": types,
            "incompatibility_mode": incompatibility_mode
        }

    def prepare_payload(self):
        self.prepare_exclusive()
        incompatibilities = self.create_incompatibilities()

        logging.info(incompatibilities)

        models = self.extract_models()
        model_objects = self.create_model_objects(models, self.data, self.get_template_content())

        merged_payload = {}

        # Loop through each model object and update its template
        for model_obj in model_objects:
            model_obj.update_template()
            model_obj.update_data()
            model_obj.create_payload()
            # Get the model name as the key for the payload
            model_name = model_obj.get_name()
            # Get the payload for the current model object
            model_payload = model_obj.get_payload()
            # Merge the payload into the merged_payload dictionary
            merged_payload[model_name] = model_payload

        current_date = date.today()

        # Set start time to 00:00:00 of the current day
        start_time = datetime.combine(current_date, datetime.min.time()).replace(microsecond=0).isoformat() + "Z"

        # Set end time to 23:59:59 of the next day
        next_day = current_date + timedelta(days=1)
        end_time = datetime.combine(next_day, datetime.max.time()).replace(microsecond=0).isoformat() + "Z"

        # Get the minimum 'from' time and maximum 'to' time from the time windows of records
        min_from_time = min(record['time_window']['from'] for record in self.data['records'])
        max_to_time = max(record['time_window']['to'] for record in self.data['records'])

        # Convert the string times to datetime objects
        min_from_time_dt = datetime.fromisoformat(min_from_time[:-1])  # Remove 'Z' from the end
        max_to_time_dt = datetime.fromisoformat(max_to_time[:-1])  # Remove 'Z' from the end

        # Adjust start and end times
        start_time = min_from_time_dt - timedelta(hours=4)
        end_time = max_to_time_dt

        # Convert start and end times back to ISO 8601 format with 'Z' appended
        start_time_iso = start_time.replace(microsecond=0).isoformat() + "Z"
        end_time_iso = end_time.replace(microsecond=0).isoformat() + "Z"

        # Remove nanos from merged_payload if already present
        if "nanos" in merged_payload:
            del merged_payload["nanos"]

        if not isinstance(incompatibilities, list):
            incompatibilities = [incompatibilities]

        merged_payload["globalStartTime"] = start_time_iso
        merged_payload["globalEndTime"] = end_time_iso
        merged_payload["shipmentTypeIncompatibilities"] = incompatibilities

        # Wrap the merged payload within a 'model' object
        return self.model_parse(merged_payload)

    def create_model_objects(self, models, data, template):
        model_objects = []
        for model in models:
            # Remove 's' from model name if it ends with 's'
            if model.endswith('s'):
                model = model[:-1]
            class_name = model.capitalize()  # Capitalize the first letter
            class_ = globals().get(class_name)  # Access class directly
            if class_:
                # Create an instance of the class without passing any data
                model_instance = class_()
                # Set the data and template attributes
                model_instance.set_data(data)
                model_instance.set_template(template)
                model_objects.append(model_instance)
                logging.info(f"Created an instance of {class_name}.")
            else:
                logging.error(f"Class not found for model: {model}")
        return model_objects

    def get_value_from_data(self, path):
        keys = path.split('.')
        current_data = self.data
        for key in keys:
            if isinstance(current_data, dict) and key in current_data:
                current_data = current_data[key]
            elif isinstance(current_data, list) and key.isdigit():
                index = int(key)
                if index < len(current_data):
                    current_data = current_data[index]
                else:
                    return None
            else:
                return None
        return current_data

    def callCFR(self, cfr_payload: dict) -> dict:
        """Call the sync api for fleet routing."""
        fleet_routing_client = optimization_v1.FleetRoutingClient()

        # Convert the data dictionary to a JSON string
        data_json = json.dumps(cfr_payload)

        # Convert the JSON string to the OptimizeToursRequest object
        fleet_routing_request = optimization_v1.OptimizeToursRequest.from_json(data_json)

        # Send the request and get the response.
        # Fleet Routing will return a response by the earliest of the `timeout`
        # field in the request payload and the gRPC timeout specified below.
        response = fleet_routing_client.optimize_tours(request=fleet_routing_request, timeout=100)
        # Convert response to JSON format.
        response_json = optimization_v1.OptimizeToursResponse.to_json(response)
        optimized_response = json.loads(response_json)
        # Map the optimization response
        mapped_response = self.map_optimization_response(optimized_response, self.data)


        return mapped_response

    def map_optimization_response(self, response: dict, data) -> dict[str, list[dict]]:

        prepared_directions = self.prepare_directions(response, data)
        result = {}

        # Create dictionaries to map order names to pickup/dropoff locations and vehicle labels to initial locations
        order_locations = {
            record['label']:
                {
                    'pickup': record['pickup'],
                    'dropoff': record['dropoff'],
                    'label': record['label'],
                    'display_name': record['display_name'],
                    'required_vehicle_type': record['required_vehicle_type'],
                    'time_window': record['time_window'],
                    'customer': record['customer'],
                    'capacity': record['capacity'],
                    'exclusive': record['exclusive'],
                    'check_in_time': record['check_in_time'],

                }
            for record in data.get('records', [])}
        vehicle_locations = {vehicle['label']: {'lat': vehicle['lat'], 'lng': vehicle['lng']}
                             for vehicle in data.get('vehicles', [])}

        totalMetrics = response.get('metrics', [])

        totalMetrics = {
            "number_of_assigned_shipments": totalMetrics['aggregatedRouteMetrics']['performedShipmentCount'],
            "total_travel_duration": int(totalMetrics['aggregatedRouteMetrics']['travelDuration'][:-1]),
            "total_wait_duration": int(totalMetrics['aggregatedRouteMetrics']['waitDuration'][:-1]),
            "total_load_duration": int(totalMetrics['aggregatedRouteMetrics']['visitDuration'][:-1]),
            "total_duration": int(totalMetrics['aggregatedRouteMetrics']['totalDuration'][:-1]),
            "total_distance": totalMetrics['aggregatedRouteMetrics']['travelDistanceMeters'],
            "total_used_vehicles": totalMetrics['usedVehicleCount'],
            "total_skipped_shipments": totalMetrics['skippedMandatoryShipmentCount'],
            "earliest_vehicle_start_time": totalMetrics['earliestVehicleStartTime'],
            "latest_vehicle_end_time": totalMetrics['latestVehicleEndTime'],
        }

        for route in response.get('routes', []):
            vehicle_label = route['vehicleLabel']
            visits = route.get('visits', [])
            metricsPerVeh = route.get('metrics', [])
            transitions = route.get('transitions', [])

            if transitions:  # Check if transitions is not empty
                # prepare first transition
                transitions[0]["travelDuration"] = "0s"
                transitions[0]["travelDistanceMeters"] = 0
                transitions[0]["waitDuration"] = "0s"
                transitions[0]["totalDuration"] = "0s"
                transitions[0]["startTime"] = visits[0]['startTime']

            for i, visit in enumerate(visits):
                visit['id'] = i

            if not visits:
                continue

            steps = []

            # Add the driver's initial location as the first step
            initial_location = vehicle_locations.get(vehicle_label)
            if initial_location:
                steps.append({'action_type': 'start', 'lat': initial_location['lat'], 'lng': initial_location['lng']})

            # Process each visit
            for visit in visits:
                action_type = 'pickup' if visit.get('isPickup', True) else 'dropoff'
                order_name = visit.get('shipmentLabel', '')

                # add any attribute from data
                location = order_locations.get(order_name, {}).get(action_type)
                customer = order_locations.get(order_name, {}).get("customer")
                exclusive = order_locations.get(order_name, {}).get("exclusive")
                check_in_time = order_locations.get(order_name, {}).get("check_in_time")

                transition_start_time = datetime.strptime(transitions[visit.get("id")]["startTime"],
                                                          "%Y-%m-%dT%H:%M:%SZ")

                # Extract travel duration in seconds and convert to int
                travel_duration_seconds = int(
                    transitions[visit.get("id")]["travelDuration"][:-1])  # remove the "s" suffix

                # Add travel duration to start time
                arrival_time = transition_start_time + timedelta(seconds=travel_duration_seconds)

                # Convert arrival time back to string in the same format
                arrival_time = arrival_time.strftime("%Y-%m-%dT%H:%M:%SZ")

                if location:
                    steps.append({
                        'action_type': action_type,
                        'arrival_time': arrival_time,
                        'waiting_duration': int(transitions[visit.get("id")]["waitDuration"][:-1]),
                        'checkin_time': visit.get('startTime', ''),
                        'checkin_duration': int(check_in_time),
                        'departure_time': transitions[visit.get("id") + 1]["startTime"],
                        'load': int(visit.get('demands', [{"value": ""}])[0].get('value', '')),
                        'order_name': order_name,
                        'lat': location['lat'],
                        'lng': location['lng'],
                        'customer': customer,
                        'exclusive': exclusive,
                        'distance': transitions[visit.get("id")]["travelDistanceMeters"],
                    })

            result[vehicle_label] = {
                'start_time': route['vehicleStartTime'],
                'end_time': route['vehicleEndTime'],
                'number_of_shipments': metricsPerVeh['performedShipmentCount'],
                'travel_duration': int(metricsPerVeh['travelDuration'][:-1]),
                'wait_duration': int(metricsPerVeh['waitDuration'][:-1]),
                'load_duration': int(metricsPerVeh['visitDuration'][:-1]),
                'total_duration': int(metricsPerVeh['totalDuration'][:-1]),
                'total_distance': metricsPerVeh['travelDistanceMeters'],
                'steps': steps
            }

            # Complete the loop by logging latitude and longitude for each step
            ordered_array = []
            for i in range(len(steps)):
                if i < len(steps) - 1:
                    start_step = steps[i]
                    stop_step = steps[i + 1]
                    ordered_array.append(start_step)
                    # Call the function with the start and stop locations
                    start_lat = float(start_step['lat'])
                    start_lon = float(start_step['lng'])
                    stop_lat = float(stop_step['lat'])
                    stop_lon = float(stop_step['lng'])
                    country = "uae"

                    # Check if start and stop locations are the same
                    if (start_lat, start_lon) == (stop_lat, stop_lon):
                        continue  # Skip calling the ETA API if locations are the same

                    eta_response = self.find_direction(start_lat, start_lon, stop_lat, stop_lon, prepared_directions)

                    # Check if eta_response contains the expected key
                    if 'directions_data' in eta_response:
                        # Extract directions_data
                        directions_data = eta_response.get('directions_data', [])
                        # Log the count of directions_data
                        logging.info(f"Type of directions_data: {type(directions_data)}")
                        logging.info(f"Count of directions data: {len(directions_data)}")

                        if isinstance(directions_data, str):
                            try:
                                directions_data = json.loads(directions_data)
                                logging.info("Parsed directions_data from string to list of dictionaries.")
                            except json.JSONDecodeError:
                                logging.error("Error parsing directions_data as JSON string.")
                                continue

                        logging.info(f"Type of directions_data: {type(directions_data)}")

                        ordered_array.extend(directions_data)

                        # Log the current total count of ordered_array
                        logging.info(f"Current total count of ordered_array: {len(ordered_array)}")
                    else:
                        # Log the eta_response in case directions_data is not found
                        logging.error(f"Key 'directions_data' not found in eta_response. Full response: {eta_response}")
                else:
                    ordered_array.append(stop_step)

            pickup_index = next(
                (index for index, item in enumerate(ordered_array) if item.get('action_type') == 'pickup'), None)

            if pickup_index is not None:
                # Slice the array to remove elements before the first "pickup"
                ordered_array = ordered_array[pickup_index:]

            result[vehicle_label]['steps'] = ordered_array

        return result

    def prepare_directions(self, response: dict, data):

        # Mapping
        order_locations = {record['label']: {'pickup': record['pickup'], 'dropoff': record['dropoff']}
                           for record in data.get('records', [])}
        vehicle_locations = {vehicle['label']: {'lat': vehicle['lat'], 'lng': vehicle['lng']}
                             for vehicle in data.get('vehicles', [])}

        eta_calls = []

        for route in response.get('routes', []):
            vehicle_label = route['vehicleLabel']
            visits = route.get('visits', [])

            if not visits:
                continue

            steps = []

            # Initial location
            initial_location = vehicle_locations.get(vehicle_label)
            if initial_location:
                steps.append({'action_type': 'start', 'lat': initial_location['lat'], 'lng': initial_location['lng']})

            # Process visits
            for visit in visits:
                action_type = 'pickup' if visit.get('isPickup', True) else 'dropoff'
                order_name = visit.get('shipmentLabel', '')
                location = order_locations.get(order_name, {}).get(action_type)

                if location:
                    steps.append({
                        'action_type': action_type,
                        'start_time': visit.get('startTime', ''),
                        'load': visit.get('demands', [{"value": ""}])[0].get('value', ''),
                        'order_name': order_name,
                        'lat': location['lat'],
                        'lng': location['lng']
                    })

            # Schedule ETA API calls in threads
            for i in range(len(steps) - 1):
                start_step = steps[i]
                stop_step = steps[i + 1]
                start_lat = float(start_step['lat'])
                start_lon = float(start_step['lng'])
                stop_lat = float(stop_step['lat'])
                stop_lon = float(stop_step['lng'])
                country = "uae"

                if (start_lat, start_lon) != (stop_lat, stop_lon):
                    eta_calls.append((start_lat, start_lon, stop_lat, stop_lon, country))

        # Execute all ETA API calls in parallel
        all_responses = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_eta = {
                executor.submit(self.call_eta_api, start_lat, start_lon, stop_lat, stop_lon, country): (
                    start_lat, start_lon, stop_lat, stop_lon)
                for start_lat, start_lon, stop_lat, stop_lon, country in eta_calls
            }

            for future in as_completed(future_to_eta):
                start_lat, start_lon, stop_lat, stop_lon = future_to_eta[future]
                try:
                    response = future.result()
                    all_responses.append({
                        "start_lat": start_lat,
                        "end_lat": stop_lat,
                        "start_lng": start_lon,
                        "end_lng": stop_lon,
                        "response": response
                    })
                except Exception as exc:
                    print(f'API call generated an exception: {exc}')

        return all_responses

    def find_direction(self, start_lat, start_lon, stop_lat, stop_lon, prepared_directions=[]):
        # Iterate through each direction in prepared_directions
        for direction in prepared_directions:
            # Check if the start and stop coordinates match the current direction
            if (direction["start_lat"] == start_lat and
                    direction["start_lng"] == start_lon and
                    direction["end_lat"] == stop_lat and
                    direction["end_lng"] == stop_lon):
                # If a match is found, return the corresponding response
                return direction["response"]
        # If no match is found, return None or an appropriate response
        return None

    def call_eta_api(self, start_lat, start_lon, stop_lat, stop_lon, country):

        endpoint = "ennv"
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        query_params = {
            'country': country,
            'start_lon': start_lon,
            'stop_lon': stop_lon,
            'start_lat': start_lat,
            'stop_lat': stop_lat,
            'source': 'mobile',
            'action': 'GetAll'
        }
        response = requests.get(endpoint, headers=headers, params=query_params)
        return json.loads(response.text)

    def match_vehicles_types(self, cfr_payload):
        updated_cfr_payload = json.loads(json.dumps(cfr_payload))  # Deep copy to avoid modifying original payload
        for shipment in updated_cfr_payload['model']['shipments']:
            vehicle_type = shipment.get('vehicle_type')
            allowed_vehicle_indices = []
            for vehicle in updated_cfr_payload['model']['vehicles']:
                # Split the label by comma and take the first element as the vehicle type
                if vehicle.get('label').split(',')[0] == vehicle_type:
                    allowed_vehicle_indices.append(vehicle.get('index'))
            shipment['allowed_vehicle_indices'] = allowed_vehicle_indices
            del shipment['vehicle_type']  # Remove the 'vehicle_type' field from each shipment
        # Remove the 'index' field from each vehicle
        for vehicle in updated_cfr_payload['model']['vehicles']:
            del vehicle['index']
        # Call the function to prepare labels
        self.prepare_labels(updated_cfr_payload)
        return updated_cfr_payload

    def prepare_labels(self, cfr_payload):
        for shipment in cfr_payload['model']['shipments']:
            for i, label_part in enumerate(shipment['label'].split(',')):
                if i > 0:
                    shipment['label'] = label_part.strip()
        for vehicle in cfr_payload['model']['vehicles']:
            for i, label_part in enumerate(vehicle['label'].split(',')):
                if i > 0:
                    vehicle['label'] = label_part.strip()
