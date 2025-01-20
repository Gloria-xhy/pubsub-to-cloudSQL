from setuptools import setup, find_packages

setup(
    name='qponqpon',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'pymysql'
    ],
    description='A project that uses Apache Beam to process data from Pub/Sub and interacts with Cloud SQL.',
    classifiers=[
        'Programming Language :: Python :: 3',
        'Operating System :: OS Independent',
    ],
)
