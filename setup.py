from setuptools import setup, find_packages


running_requirements =[
    'requests==2.7.0',
    'httpretty==0.8.10',
    'boto==2.34.0',
    'botocore==1.3.29',
    'psycopg2==2.6',
    'SQLAlchemy==1.0.5',
    'Cython==0.24.1',
    'pandas>=0.14.0'
]

setup(
    name='aws_etl_tools',
    description='some helpers for getting your data into redshift',
    url='https://github.com/shopkeep/aws_etl_tools',
    author_email='data@shopkeep.com',
    version='0.0.1',
    install_requires=running_requirements,
    tests_require=running_requirements + [
        'freezegun==0.3.5',
        'moto==0.4.1',
        'nose==1.3.7',
        'rednose==0.4.3'
    ],
    packages=find_packages(),
    include_package_data=True,
    test_suite='nose.collector'
)
