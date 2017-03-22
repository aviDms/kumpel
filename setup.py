from setuptools import setup, find_packages

setup(
    name = 'kumpel',
    version = '0.11',
    description = 'ETL packages bundle for Python.',
    long_description = '',
    classifiers = [
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.5',
        'Topic :: Database',
    ],
    keywords = 'etl business intelligence bigquery data',
    url = 'http://github.com/avidms/kumpel',
    author = 'Avram Dames',
    author_email = 'avram.dames@gmail.com',
    license = 'MIT',
    packages = find_packages(),
    install_requires = [
        'bigquery-python',
    ],
    include_package_data = True,
    zip_safe = False
)
