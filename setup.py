#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = ['Click>=7.0', ]

test_requirements = [ ]

setup(
    author="Mattia Samory",
    author_email='mattia.samory@gmail.com',
    python_requires='>=3.6',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description="Download and extract data from Pushshift's [archive files](https://files.pushshift.io/reddit).",
    entry_points={
        'console_scripts': [
            'pullshift=pullshift.cli:main',
        ],
    },
    install_requires=requirements,
    license="MIT license",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='pullshift',
    name='pullshift',
    packages=find_packages(include=['pullshift', 'pullshift.*']),
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/hide_ous/pullshift',
    version='0.1.0',
    zip_safe=False,
)
