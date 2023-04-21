from setuptools import setup, find_packages
from typing import List

PROJECT_NAME= "finance_complaint"
VERSION="0.0.3"
AUTHOR= "Shivansh Kaushal"
DESCRIPTION= "This is a ...."
REQUIREMENT_FILE_NAME= "requirements.txt"
HYPHEN_E_DOT= "-e ."

def get_requirements_list(file_path=REQUIREMENT_FILE_NAME)-> List[str]:
    with open(file_path) as requirement_file:
        requirement_list = requirement_file.readlines()
        requirement_list = [requirement_name.replace("\n","") for requirement_name in requirement_list]
        if HYPHEN_E_DOT in requirement_list:
            requirement_list.remove(HYPHEN_E_DOT)
        return requirement_list

setup(
    name=PROJECT_NAME,
    version=VERSION,
    author=AUTHOR,
    description=DESCRIPTION,
    packages=find_packages(),
    install_requires=get_requirements_list()
)