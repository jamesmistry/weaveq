import weaveq.build_constants
import setuptools

setuptools.setup(name='weaveq',
    version="{0}.{1}.{2}{3}".format(weaveq.build_constants.major_version, weaveq.build_constants.minor_version, weaveq.build_constants.release_version, weaveq.build_constants.release_phase),
    description="A program and module for pivoting and joining across collections of Python objects, with special support for pivoting and joining JSON files, CSV files and Elasticsearch resultsets",
    url=weaveq.build_constants.project_url,
    author='James Mistry',
    author_email='',
    entry_points = {
        'console_scripts': ['weaveq=weaveq.__main__:main'],
    },
    license='MIT',
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Development Status :: 5 - Production/Stable",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Intended Audience :: System Administrators",
        "Natural Language :: English",
        "Operating System :: POSIX :: Linux",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    keywords="weaveq elasticsearch json csv pivot join correlate cross-reference",
    install_requires=[
        "elasticsearch >= 5.1.0",
        "elasticsearch_dsl >= 5.1.0",
        "pyparsing >= 2.2.0",
        "six >= 1.5.2",
    ],
    packages=['weaveq'],
    zip_safe=False)
