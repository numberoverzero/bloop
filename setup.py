""" Setup file """
import os

from setuptools import setup


HERE = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(HERE, "README.rst")) as f:
    README = f.read()

with open(os.path.join(HERE, "CHANGELOG.rst")) as f:
    CHANGES = f.read()


def get_version():
    with open(os.path.join(HERE, "bloop/__init__.py")) as f:
        for line in f:
            if line.startswith("__version__"):
                return eval(line.split("=")[-1])

PACKAGES = [
    "bloop",
    "bloop.ext",
    "bloop.stream"
]

REQUIREMENTS = [
    "blinker==1.4",
    "boto3==1.4.1",
    "declare==0.9.11",
]

if __name__ == "__main__":
    setup(
        name="bloop",
        version=get_version(),
        description="ORM for DynamoDB",
        long_description=README + "\n\n" + CHANGES,
        classifiers=[
            "Development Status :: 5 - Production/Stable",
            "Intended Audience :: Developers",
            "License :: OSI Approved :: MIT License",
            "Operating System :: OS Independent",
            "Programming Language :: Python",
            "Programming Language :: Python :: 3",
            "Programming Language :: Python :: 3.5",
            "Topic :: Software Development :: Libraries"
        ],
        author="Joe Cross",
        author_email="joe.mcross@gmail.com",
        url="https://github.com/numberoverzero/bloop",
        license="MIT",
        keywords="aws dynamo dynamodb dynamodbstreams orm",
        platforms="any",
        include_package_data=True,
        packages=PACKAGES,
        install_requires=REQUIREMENTS,
    )
