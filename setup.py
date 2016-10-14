from setuptools import setup, find_packages


setup(
    name='aws_etl_tools',
    description='some helpers for getting your data into redshift',
    url='https://github.com/shopkeep/aws_etl_tools',
    author_email='data@shopkeep.com',
    install_requires=[
        'requests==2.7.0',
        'boto==2.34.0',
        'botocore==1.3.29',
        'psycopg2==2.6',
        'SQLAlchemy==1.0.5',
        'Cython==0.24.1',
        'pandas>=0.14.0'
    ],
    version='0.0.1',
    packages=find_packages(),
    test_suite='nose.collector'
)
