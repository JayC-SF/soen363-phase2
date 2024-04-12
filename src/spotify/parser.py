import json
import os
from typing import Dict, TypeVar, Type

from spotify.util import setup_spotify_folders
from utility.mapper import JsonToObjectMapper

T = TypeVar('T')


class SpotifyParser:
    """
    A parser class for Spotify data, specifically for parsing spotify information.

    This class is responsible for parsing JSON files that contain spotify data, converting
    them into instances of a specified model class.

    Attributes:
        __items_folder_path (str): The path to the directory containing JSON data files.
        __mapper_file_path (str): The path to the JSON file that contains mapping information
                                  used to map JSON data to the model class attributes.
        __cls (Type[T]): The class type (model) into which the JSON data will be parsed.
        __mapper (JsonToObjectMapper | None): An instance of JsonToObjectMapper used to map JSON
                                              data to model instances.
        __mapping (Dict[str, str]): A dictionary that holds the mapping from JSON data fields
                                    to model attributes.

    Methods:
        __init__(self, folder_name, cls: Type[T]): Initializes a new SpotifyParser instance.
        __load_mapping(self): Loads the mapping information from a JSON file.
        parse_all(self): Parses all JSON files in the specified directory and converts them
                         to instances of the model class.
        parse_single(self, file_path) -> T: Parses a single JSON file and converts it to an
                                            instance of the model class.
    """

    def __init__(self, folder_name, cls: Type[T]):
        """
        Initializes a new instance of the SpotifyParser class.

        Parameters:
            folder_name (str): The name of the folder containing JSON files to parse.
            cls (Type[T]): The class type into which the JSON data will be parsed. This class
                           should have an initializer that accepts keyword arguments corresponding
                           to the attributes mapped from the JSON data.
        """
        _, _, self.__items_folder_path, self.__mapper_file_path = setup_spotify_folders(folder_name)
        self.__cls = cls
        self.__mapper: JsonToObjectMapper | None = None
        self.__mapping: Dict[str: str] = None
        self.__load_mapping()

    def __load_mapping(self):
        """
        Loads mapping data from a JSON file specified by `self.__mapper_file_path`.

        This method sets the `self.__mapper` attribute with a JsonToObjectMapper instance configured
        with the loaded mapping data.
        """
        with open(self.__mapper_file_path, 'r') as f:
            self.__mapping = json.load(f)
            self.__mapper = JsonToObjectMapper(self.__mapping)

    def parse_all(self, middleware=None, exlude_files=[]):
        """
        Parses all JSON files in the `self.__items_folder_path` directory, converting each into
        an instance of `self.__cls`. An optional middleware function can be provided to modify
        each object after it is mapped from the JSON data.

        The middleware function should take two arguments: the mapped object and the original JSON data,
        and it should return the modified object. If no middleware is provided, objects are returned as mapped
        without any additional modifications.

        Parameters:
            middleware (Callable[[T, Dict], T]): A function that takes the mapped object and the original
                                                 JSON data, modifies the mapped object, and returns it.

        Returns:
            A list of instances of `self.__cls`, each corresponding to a JSON file in the directory.
            If no files are found or an error occurs, it may return None or an incomplete list.

        Example:
            Using a middleware lambda function to modify an attribute of the mapped objects:

            ```python
            albums: List[AlbumModel] = parser.parse_all(
                middleware=lambda mapped_object, json_data: (
                    setattr(mapped_object, 'attribute_name', 'value') or mapped_object
                )
            )
            ```

            This middleware function sets the 'attribute_name' of each mapped_object to 'value'
            and ensures the modified object is returned.
        """
        if not os.path.exists(self.__items_folder_path):
            print(f"No directory was found at {self.__items_folder_path}")
            return

        files = [f for f in os.listdir(self.__items_folder_path) if
                 os.path.isfile(os.path.join(self.__items_folder_path, f)) and f.endswith('.json')]
        if not files:
            print(f"No data was found at {self.__items_folder_path}")
            return

        object_list = []
        for file in files:
            file_path = os.path.join(self.__items_folder_path, file)
            mapped_object = self.parse_single(file_path)
            if mapped_object:
                if middleware:
                    # Pass the mapped object and the original JSON data to the middleware function
                    with open(file_path, 'r') as f:
                        json_data = json.load(f)
                    mapped_object = middleware(mapped_object, json_data)
                object_list.append(mapped_object)
        return object_list

    def parse_single(self, file_path) -> T:
        """
          Parses a single JSON file into an instance of `self.__cls`.

          Parameters:
              file_path (str): The path to the JSON file to be parsed.

          Returns:
              An instance of `self.__cls` initialized with data from the JSON file. Returns None
              if the file is empty, cannot be decoded, or an error occurs during parsing.
          """
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
                if data:  # Check if data is not empty
                    return self.__mapper.map(data, self.__cls)
                else:
                    print(f"Warning: Empty JSON file skipped - {file_path}")
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON from {file_path}: {e}")
        except Exception as e:
            print(f"Unexpected error while processing {file_path}: {e}")
        return None
