import os
from setuptools import setup, find_packages


setup(
    name='zfstools',
    version='1.0.0.dev0',
    description="ZFS tools including snapshot management and snapshot replay.",
    url='',
    
    packages=find_packages(exclude=['build*', 'tests*']),
    include_package_data=True,
    
    author='Mike Boers',
    author_email='floss+zfstools@mikeboers.com',
    license='BSD-3',
    
    entry_points={
        'console_scripts': '''

            zfs-replay = zfstools.replay.__main__:main

        ''',
    },

    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    
)
