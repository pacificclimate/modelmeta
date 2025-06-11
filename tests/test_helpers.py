from importlib import import_module
from importlib.resources import files


def resource_filename(package, path):
    return str(files(import_module(package)) / path)
