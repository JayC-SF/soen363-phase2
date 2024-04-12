import json
from typing import Type, TypeVar, Any, Dict

T = TypeVar('T')


class JsonToObjectMapper:
    def __init__(self, mapping: Dict[str, str]):
        """
        Initialize the mapper with a specific mapping.

        :param mapping: A dictionary representing the mapping from object attributes to JSON paths.
        """
        self.mapping = mapping

    def extract_value(self, data, path):
        """
        Extracts a value from the nested JSON data based on the given path.

        :param data: The JSON data as a nested dictionary.
        :param path: The string path representing where to look for the desired data.
        :return: The extracted data.
        """
        keys = path.split('.')
        for key in keys:
            if '[]' in key:
                key, subkey = key.replace('[]', ''), None
                if '.' in key:
                    key, subkey = key.split('.')
                value = data.get(key, [])
                if isinstance(value, list) and subkey:
                    return [subitem.get(subkey, None) for subitem in value if subitem and subkey in subitem]
                return [subitem for subitem in value if subitem]
            else:
                if isinstance(data, dict):
                    data = data.get(key, {})
                else:
                    return None
        return data

    def map(self, json_data: Dict[str, Any], cls: Type[T]) -> T:
        """
        Maps JSON data to an object of the specified class based on the provided mapping.

        :param json_data: A dictionary representing an object.
        :param cls: The class type to which the JSON data is to be mapped.
        :return: An instance of cls filled with data from the json_data.
        """
        obj_data = {}
        for attr, json_path in self.mapping.items():
            value = self.extract_value(json_data, json_path)
            obj_data[attr] = value

        return cls(**obj_data)
