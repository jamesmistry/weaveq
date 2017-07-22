WeaveQ
=======

WeaveQ: A program and module for pivoting and joining across collections of 
data, with special support for pivoting and joining across JSON files, CSV 
files and Elasticsearch resultsets.

Getting Started
---------------

**Prerequisites**

In order to install WeaveQ you will need:

- Python 2.7 or higher (WeaveQ runs under Python 3)
- pip, a Python package installer (the pip package is usually called 
  ``python-pip`` in Linux package repositories)

For development of WeaveQ, you will need:

- tox >= 2.7.0 (available from PyPi)
- six >= 1.5.2 (available from PyPi)
- pyparsing >= 2.2.0 (available from PyPi)
- elasticsearch >= 5.1.0 (available from PyPi)
- elasticsearch_dsl >= 5.1.0 (available from PyPi)
- Doxygen (optional, available from your OS package repository)
- Sphinx (optional, available from PyPi)

**Installing**

To install the latest stable WeaveQ, use pip (you may need to ``sudo``)::

    $ pip install weaveq

To install from source, run the ``setup.py`` file located in the WeaveQ 
repository root (you may need to ``sudo``)::

    $ python setup.py install

**Documentation**

Comprehensive WeaveQ documentation is located at: <https://readthedocs.org/projects/weaveq/>

To build the documentation in the WeaveQ repository locally, from the repository root::

    $ doxygen
    $ cd docs
    $ make html

In-code HTML documentation will be written to ``docs/code/html`` and 
user/developer HTML documentation will be written to ``docs/build/html``.

Running the Tests
-----------------

There are 3 types of WeaveQ tests:

1. **Unit:** Exercises individual classes.

2. **System:** Exercises unit integration, integration with external modules 
   and integration with Elasticsearch.

3. **Performance:** Measures average performance of pre-defined operations to 
   help gauge performance impact of code changes.

For the system tests to work correctly, you need to have an Elasticsearch 
instance accessible from your development system (a vanilla, single local 
node should be sufficient). You will have to configure the system tests to 
point to the Elasticsearch node - do this by entering the host and port in 
the ``tests/system/elastic_test_config.json`` file.

To run the unit and system tests, from the repository root run::

    $ tox
    
``tox`` is used to manage virtual environments for testing that WeaveQ installs 
and runs with multiple Python versions. To successfully test both Python 2.7 
and Python 3 compatibility, you need to have these interpreters installed on 
your system.

Note in particular that WeaveQ compatibility with the following has not been 
tested in this release (though this doesn't necessarily mean it won't work):

- Python versions < 2.7.6 or > 3.4
- Operating systems other than Linux
- Elasticsearch versions < 5.1.2

Deployment
----------

In order to use Elasticsearch data sources in WeaveQ queries, you must supply 
a configuration file on the command line. This file specifies the 
addresses/host names of Elasticsearch nodes, authentication certificates and 
other settings. If deploying to a controlled environment with an Elasticsearch
instance that you want users to access by default, consider providing a 
pre-written configuration file (for example, in ``/etc/weaveq``) so that users 
can get up and running quickly. You may also want to alias the ``weaveq`` 
command to specify the default configuration without requiring users to 
specify the ``--config`` option.

The configuration file format is documented at 
<https://readthedocs.org/projects/weaveq/>

Versioning
----------

WeaveQ versioning follows the SemVer specification: <http://semver.org/>

You can see the tagged WeaveQ versions at: 
<https://github.com/jamesmistry/weaveq/tags>

Licence
-------

This project is licensed under the MIT License - see the LICENSE file.


