"""Setup.py for the pandera provider package."""

from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="airflow-provider-pandera",
    version="0.0.1",
    description="Airflow provider for pandera.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    entry_points={
        "apache_airflow_provider": [
            "provider_info=pandera_provider.__init__:get_provider_info"
        ]
    },
    license="Apache License 2.0",
    packages=find_packages(exclude=["tests*"]),
    install_requires=[
        "apache-airflow>=2.0",
        "pandera>=0.13.4",
    ],
    setup_requires=["setuptools", "wheel"],
    author="Eric Hamers",
    author_email="erichamers@gmail.com",
    url="https://www.union.ai/pandera",
    classifiers=[
        "Framework :: Apache Airflow",
        "Framework :: Apache Airflow :: Provider",
        "Development Status :: 5 - Production/Stable",
        "Operating System :: OS Independent",
        "License :: OSI Approved :: MIT License",
        "Intended Audience :: Science/Research",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Scientific/Engineering",
    ],
    python_requires="~=3.7",
)
