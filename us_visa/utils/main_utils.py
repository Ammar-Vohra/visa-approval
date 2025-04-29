import os, sys
import yaml
import pickle
import numpy as np
from us_visa.exception.exception import VisaException
from us_visa.logging.logger import logging


def read_yaml_file(file_path) -> dict:
    try:
        with open(file_path, 'rb') as file:
            return yaml.safe_load(file)
    except Exception as e:
        raise VisaException(e,sys)
    

def write_yaml_file(file_path, content, replace=False) -> None:
    try:
        if replace:
            if os.path.exist(file_path):
                os.remove(file_path)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w") as file:
            yaml.dump(content, file)
    except Exception as e:
        raise VisaException(e,sys)


def save_object(file_path, object) -> None:
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "wb") as file:
            pickle.dump(object, file) 
    except Exception as e:
        raise VisaException(e, sys)

    

def save_numpy_array_data(file_path, array) -> None:
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "wb") as file:
            np.save(file, array) 
    except Exception as e:
        raise VisaException(e, sys)
    

def load_object(file_path):
    try:
        if not os.path.exists(file_path):
            raise Exception(f"The filepath: {file_path} does not exist")
        
        with open(file_path, "rb") as file:
            print(file)
            return pickle.load(file)
    except Exception as e:
        raise VisaException(e,sys)


def load_numpy_array_data(file_path):
    try:
        with open(file_path, "rb") as file:
            return np.load(file)   # Correct order
    except Exception as e:
        raise VisaException(e, sys)