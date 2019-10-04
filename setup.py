from setuptools import setup, find_packages


running_requirements =[
    'boto3==1.7.84',
    'botocore==1.10.84',
    'Cython==0.24.1',
    'httpretty==0.8.10',
    'pandas>=0.14.0',
    'psycopg2==2.7',
    'requests==2.20.0',
    'SQLAlchemy==1.0.5'
]

setup(
    name='aws_etl_tools',
    description='some helpers for getting your data into redshift',
    url='https://github.com/shopkeep/aws_etl_tools',
    author_email='data@shopkeep.com',
    version='0.0.1',
    install_requires=running_requirements,
    tests_require=running_requirements + [
        'boto==2.45.0',
        'freezegun==0.3.5',
        'moto==0.4.23',
        'nose==1.3.7',
        'rednose==0.4.3'
    ],
    packages=find_packages(),
    include_package_data=True,
    test_suite='nose.collector'
)
