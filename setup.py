from setuptools import setup

with open('README.md', encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='bigquery-fdw',
    version='2.2.0',
    description='BigQuery Foreign Data Wrapper for PostgreSQL',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Gabriel Bordeaux',
    author_email='pypi@gab.lc',
    url='https://github.com/gabfl/bigquery_fdw',
    license='MIT',
    python_requires='>=3.10',
    packages=['bigquery_fdw'],
    package_dir={'bigquery_fdw': 'src'},
    # external dependencies
    install_requires=[
        'google-cloud-bigquery==3.47.0',
        'google-auth==2.62.2',
        'google-auth-oauthlib==1.5.0',
        # 'protobuf==4.21.8',  # Forcing version for google-cloud-bigquery
        # 'grpcio==1.60.1',  # Forcing version for google-cloud-bigquery
        # 'grpcio-status==1.60.1',  # Forcing version for google-cloud-bigquery
    ],
    entry_points={
        'console_scripts': [
            # 'bigquery_fdw = bigquery_fdw.fdw:main',
            'bq_client_test = bigquery_fdw.bqclient_test:main',
        ],
    },
    classifiers=[  # see https://pypi.python.org/pypi?%3Aaction=list_classifiers
        'Topic :: Database',
        'Topic :: Database :: Database Engines/Servers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: MacOS',
        'Operating System :: POSIX :: Linux',
        'Natural Language :: English',
        #  'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python',
        'Development Status :: 4 - Beta',
        #  'Development Status :: 5 - Production/Stable',
    ],
)
