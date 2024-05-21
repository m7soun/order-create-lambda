import json
import logging
import re
import copy


class Shipment:
    flattened_data_array = []
    flattened_data_dict = {}

    def __init__(self):
        self._data = None
        self._template = None
        self._payload = None
        self._name = "shipments"

    def get_name(self):
        return self._name

    def set_data(self, data):
        self._data = data

    def get_data(self):
        return self._data

    def set_template(self, template):
        self._template = template

    def get_template(self):
        return self._template

    def update_template(self):
        if 'model' in self._template:
            class_name = self.__class__.__name__
            class_key = class_name.lower() + 's'  # Assuming class names are singular
            if class_key in self._template['model']:
                self._template = self._template['model'][class_key]
                return True
            else:
                print(f"No element found for class '{class_name}' in the template.")
                return False
        else:
            print("No 'model' element found in the template.")
            return False

    def get_flattened_data_array(self):
        return self.flattened_data_array

    def get_flattened_data_dict(self):
        return self.flattened_data_dict

    def get_first_components(self, obj):
        first_components = set()
        if isinstance(obj, dict):
            # If obj is a dictionary, iterate over its values
            for value in obj.values():
                first_components.update(self.get_first_components(value))
        elif isinstance(obj, list):
            # If obj is a list, iterate over its elements
            for item in obj:
                first_components.update(self.get_first_components(item))
        elif isinstance(obj, str):
            # If obj is a string, extract the contents within {{...}} using regular expression
            matches = re.findall(r'{{(.*?)}}', obj)
            for match in matches:
                # Split the match by '.' to extract the first component
                components = match.split('.')
                first_components.add(components[0])
        return first_components

    def validate_template(self):
        first_components = self.get_first_components(self._template)
        print(first_components)
        if len(first_components) != 1:
            # If not all paths have the same first component, raise an error
            raise ValueError("Not all paths have the same first component")

    def list_of_data_elements(self):
        self.validate_template()
        return self.get_first_components(self._template)

    def get_data_element(self):
        # Get the list of data elements
        data_elements = self.list_of_data_elements()

        # As list_of_data_elements always returns one element, retrieve the first element
        first_data_element = next(iter(data_elements), None)

        # Retrieve the corresponding data element from _data
        if first_data_element:
            return self._data.get(first_data_element)
        else:
            return None

    def update_data(self):
        # Get the data element
        data_element = self.get_data_element()

        # Update _data with the retrieved data element
        if data_element is not None:
            self._data = data_element
            return True
        else:
            print("No data element found.")
            return False

    def create_payload(self):
        payload = []
        if self._template and self._data:
            for data_element in self._data:
                # Merge the template with the current data element
                payload_element = self._template[0]
                payload.append(payload_element)
        else:
            print("Template or data is missing.")
        self._payload = payload

        self.flatten_data()
        self.resolve_payload_placeholders()

        return self._payload

    def flatten_data(self):
        # Clear the previous flattened data
        self.flattened_data_dict.clear()

        data = self._data
        for i, item in enumerate(data):
            self.flatten_dict_recursive(item, str(i))
        return self.flattened_data_dict

    def flatten_dict_recursive(self, d, path):
        for k, v in d.items():
            new_path = f"{path}.{k}"
            if isinstance(v, dict):
                # If the value is a dictionary, recursively flatten it
                self.flatten_dict_recursive(v, new_path)
            else:
                # If the value is not a dictionary, add it to the flattened data dictionary
                self.flattened_data_dict[new_path] = v

    def resolve_payload_placeholders(self):
        resolved_payload = []
        flattened_data_dict = self.get_flattened_data_dict()
        # Iterate over each element in the _payload
        for i, element in enumerate(self._payload):
            # Convert the element to a string to apply regular expressions
            element_str = json.dumps(element)
            # Find all placeholders in the element
            placeholders = re.findall(r'{{(.*?)}}', element_str)
            # Iterate over each placeholder found
            for placeholder in placeholders:
                # Split the placeholder to get the path
                path = placeholder.split('.')
                # Remove the first element from the path (e.g., records)
                path = '.'.join(path[1:])
                # Replace * with the current payload index
                path = path.replace('*', str(i))
                # Check if the path exists in flattened_data_dict
                if path in flattened_data_dict:
                    # Replace the placeholder with the value from flattened_data_dict
                    element_str = element_str.replace(f'{{{{{placeholder}}}}}', str(flattened_data_dict[path]))
                else:
                    print(f"Placeholder '{placeholder}' not found in flattened_data_dict.")
            # Convert the resolved element back to JSON
            resolved_element = json.loads(element_str)
            # Convert string representations of numbers to numeric types
            resolved_element = self.convert_string_to_number(resolved_element)
            # Append the resolved element to the new array
            resolved_payload.append(resolved_element)
        # Update the _payload with the resolved values
        self._payload = resolved_payload

    def convert_string_to_number(self, element):
        """
        Recursively convert string representations of numbers to their corresponding numeric types.
        """
        if isinstance(element, dict):
            for key, val in element.items():
                element[key] = self.convert_string_to_number(val)
        elif isinstance(element, list):
            for i, item in enumerate(element):
                element[i] = self.convert_string_to_number(item)
        elif isinstance(element, str):
            # Check if the string represents a float
            try:
                float_val = float(element)
                if float_val.is_integer():  # Check if it's an integer
                    element = int(float_val)
                else:
                    element = float_val
            except ValueError:
                pass  # If it's not a float, keep it as a string
        return element

    def replace_placeholder(self, element, placeholder, value):
        """
        Replace placeholder with value in the given element recursively.
        """
        if isinstance(element, dict):
            for key, val in element.items():
                if isinstance(val, (dict, list)):
                    element[key] = self.replace_placeholder(val, placeholder, value)
                elif isinstance(val, str):
                    element[key] = val.replace(f'{{{{{placeholder}}}}}', str(value))
        elif isinstance(element, list):
            for i, item in enumerate(element):
                element[i] = self.replace_placeholder(item, placeholder, value)
        return element

    def get_payload(self):
        return self._payload
