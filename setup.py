from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = fh.read().splitlines()

setup(
    name="metricmind",
    version="0.1.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="A benchmarking tool for comparing Dremio instances using TPC-DS queries",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/metricmind",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "metricmind=metricmind.cli:main",
        ],
    },
) 