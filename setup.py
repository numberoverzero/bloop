import os
import pathlib
from setuptools import setup, find_packages

HERE = pathlib.Path(os.path.abspath(os.path.dirname(__file__)))
README = (HERE / "README.rst").read_text()
CHANGES = (HERE / "CHANGELOG.rst").read_text()
VERSION = "VERSION-NOT-FOUND"
for line in (HERE / "bloop" / "__init__.py").read_text().split("\n"):
    if line.startswith("__version__"):
        VERSION = eval(line.split("=")[-1])

REQUIREMENTS = [
    "blinker==1.8.2",
    "boto3==1.34.131",
]

if __name__ == "__main__":
    setup(
        name="bloop",
        version=VERSION,
        description="ORM for DynamoDB",
        long_description=README + "\n\n" + CHANGES,
        long_description_content_type="text/x-rst",
        classifiers=[
            "Development Status :: 5 - Production/Stable",
            "Intended Audience :: Developers",
            "License :: OSI Approved :: MIT License",
            "Operating System :: OS Independent",
            "Programming Language :: Python",
            "Programming Language :: Python :: 3",
            "Programming Language :: Python :: 3.6",
            "Topic :: Software Development :: Libraries"
        ],
        author="Joe Cross",
        author_email="joe.mcross@gmail.com",
        url="https://github.com/numberoverzero/bloop",
        license="MIT",
        keywords="aws dynamo dynamodb dynamodbstreams orm",
        platforms="any",
        include_package_data=True,
        packages=find_packages(exclude=("docs", "examples", "scripts", "tests")),
        install_requires=REQUIREMENTS,
    )
