import os
from setuptools import setup, find_packages


setup(
    name='zfsreplay',
    version='1.0.0.dev0',
    description="ZFS snapshot replay.",
    url='',
    
    packages=find_packages(exclude=['build*', 'tests*']),
    include_package_data=True,
    
    author='Mike Boers',
    author_email='floss+zfsreplay@mikeboers.com',
    license='BSD-3',
    
    entry_points={
        'console_scripts': '''

            zfs-replay = zfsreplay.__main__:main

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
