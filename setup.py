""" Setup file """
import os

from setuptools import find_packages, setup


HERE = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(HERE, "README.rst")) as f:
    README = f.read()


def get_version():
    with open(os.path.join(HERE, "bloop/__init__.py")) as f:
        for line in f:
            if line.startswith("__version__"):
                return eval(line.split("=")[-1])

REQUIREMENTS = [
    "arrow==0.8.0",
    "blinker==1.4",
    "boto3==1.4.1",
    "declare==0.9.11"
]

TEST_REQUIREMENTS = [
    "alabaster",
    "coverage",
    "flake8",
    "pytest",
    "sphinx",
    "tox",
]

if __name__ == "__main__":
    setup(
        name="bloop",
        version=get_version(),
        description="ORM for DynamoDB",
        long_description=README,
        classifiers=[
            "Development Status :: 4 - Beta",
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
        keywords="aws dynamo dynamodb orm",
        platforms="any",
        include_package_data=True,
        packages=find_packages(exclude=("tests", "docs", "examples")),
        install_requires=REQUIREMENTS,
        tests_require=REQUIREMENTS + TEST_REQUIREMENTS,
    )
