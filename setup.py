from setuptools import setup, find_packages

setup(
    name = "vultan",
    version = "0.2",
    packages = find_packages(),
    install_requires = ['setuptools', 'pymongo>=1.5'],
    author = "Daniel Cowgill",
    author_email = "dcowgill@gmail.com",
    description = "High-level python interface for working with MongoDB documents.",
    license = "Apache License, Version 2.0",
    keywords = "mongo mongodb",
)
