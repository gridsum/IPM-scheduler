from setuptools import setup
import os

PACKAGE = "scheduler"
NAME = "ipm-scheduler"
VERSION = __import__(PACKAGE).__version__
DESCRIPTION = "ipm-scheduler(Impala Pool Memory scheduler) is used to optimize memory usage by " \
              "dynamically adjusting memory."
URL = "https://gitlab.gridsum.com/data-engineering/impala-toolbox/ipm-scheduler"
DOWNLOAD_URL = "https://gitlab.gridsum.com/data-engineering/impala-toolbox/ipm-scheduler"


setup(
    name=NAME,
    version=VERSION,
    description=DESCRIPTION,
    url=URL,
    download_url=DOWNLOAD_URL,
    packages=[
        "scheduler",
    ],
    include_package_data=True,
    zip_safe=False,
    license="Apache 2.0",
    classifiers=[
        "Development Status :: 1 - Beta",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Topic :: Software Development :: Impala :: Python Modules",
    ],
    install_requires=[
        "APScheduler",
        "certifi",
        "chardet",
        "idna",
        "numpy",
        "pandas",
        "python-dateutil",
        "pytz",
        "PyYAML",
        "requests",
        "six",
        "tornado",
        "tzlocal",
        "urllib3",
    ],
    python_requires=">=3",
    tests_require=[

    ],
    test_suite="tests",
    setup_requires=[
        "setuptools"
    ]
)

if not os.path.exists("logs"):
    os.mkdir("logs", mode=0o666)
