import io
import os
import re
import sys

from setuptools import setup

needs_pytest = 'pytest' in set(sys.argv)


def get_version():
    regex = r"__version__\s=\s\'(?P<version>[\d\.ab]+?)\'"

    path = ('async_ttl.py',)

    return re.search(regex, read(*path)).group('version')


def read(*parts):
    filename = os.path.join(os.path.abspath(os.path.dirname(__file__)), *parts)

    with io.open(filename, encoding='utf-8', mode='rt') as fp:
        return fp.read()


setup(
    name='async_ttl',
    version=get_version(),
    author='Dima Kruk',
    author_email='krkdima@gmail.com',
    url='https://github.com/krkd/async_ttl',
    description='Asyncio in memory cache with ttl',
    long_description=read('README.rst'),
    long_description_content_type='text/x-rst',
    setup_requires=['pytest-runner'] if needs_pytest else [],
    tests_require=['pytest', 'pytest-asyncio', 'pytest-cov'],
    python_requires='>=3.6.0',
    py_modules=['async_ttl'],
    include_package_data=True,
    zip_safe=False,
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    keywords=['asyncio', 'ttl', 'ttl_cache'],
)
