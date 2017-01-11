#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'mnowotka'

try:
    from setuptools import setup
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup

setup(
    name='unistruct',
    version='0.0.5',
    author='Michal Nowotka',
    author_email='mnowotka@ebi.ac.uk',
    description='Set of models, commands and utilities to implement substructure and similarity search into Unichem.',
    url='https://www.ebi.ac.uk/chembl/',
    license='Apache Software License',
    packages=['unistruct',
              'unistruct.management',
              'unistruct.management.commands',
              'unistruct.models'],
    long_description=open('README.rst').read(),
    install_requires=[
        'Django==1.5.5',
        'chembl-core-db>=0.8.4',
        'blessings>=1.6',
        'progressbar>=2.3',
        'colorama>=0.3.7',
        'termcolor>=1.1.0',
    ],
    include_package_data=False,
    classifiers=['Development Status :: 4 - Beta',
                 'Framework :: Django',
                 'Intended Audience :: Developers',
                 'License :: OSI Approved :: Apache Software License',
                 'Operating System :: POSIX :: Linux',
                 'Programming Language :: Python :: 2.7',
                 'Topic :: Scientific/Engineering :: Chemistry'],
    zip_safe=False,
)
