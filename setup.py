from setuptools import setup

if __name__ == "__main__":
    setup(
        name='pickle_cache',
        version='0.1.0',
        description='Persistent caching of Python values through pickle',
        url='http://github.com/scanner-research/pickle-cache',
        author='Will Crichton',
        author_email='wcrichto@cs.stanford.edu',
        license='Apache 2.0',
        packages=['pickle_cache'],
        setup_requires=['pytest-runner'],
        tests_require=['pytest'],
        zip_safe=False)
