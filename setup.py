import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

required_packages = ['google-cloud-bigquery', 'google-cloud-storage', 'pandas', 'tqdm']

setuptools.setup(
    name="pybq", 
    version="0.0.1",
    author="stephenleo",
    author_email="stephen.leo87@gmail.com",
    description="A python package to easily interface with Google Cloud Platform's Big Query using Python",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/stephenleo/pybq",
    install_requires=required_packages,
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6, <3.9',
)

# install_requires = ['pandas', 'google-cloud-bigquery', 'google-cloud-storage']