from setuptools import find_packages, setup

setup(
    name='legend-delta',
    version='1.0',
    author='Antoine Amend',
    author_email='antoine.amend@databricks.com',
    description='This project helps organizations define LEGEND data models that can be converted into efficient Delta '
                'pipelines',
    long_description_content_type='text/markdown',
    url='https://github.com/finos/legend-delta',
    packages=find_packages(where='.'),
    license='Apache License 2.0',
    extras_require=dict(tests=["pytest"]),
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: Databricks License',
        'Operating System :: OS Independent',
    ],
)