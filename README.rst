===============================
mysql-statsd
===============================

.. image:: https://badge.fury.io/py/mysql-statsd.png
    :target: http://badge.fury.io/py/mysql-statsd
    
.. image:: https://travis-ci.org/spilgames/mysql-statsd.png?branch=master
        :target: https://travis-ci.org/spilgames/mysql-statsd

.. image:: https://pypip.in/d/mysql-statsd/badge.png
        :target: https://crate.io/packages/mysql-statsd?version=latest


Daemon that gathers statistics from MySQL and sends them to statsd.

* Free software: BSD license
* Documentation: http://mysql-statsd.rtfd.org.


Usage / Installation
========

Install mysql_statsd through pip(pip is a python package manager, please don't use sudo!):

```
pip install mysql_statsd
```

If all went well, you'll now have a new executable called mysql_statsd in your path.


## Running mysql_statsd

```
$ mysql_statsd --config /etc/mysql-statsd.conf 
```

Assuming you placed a config file in /etc/ named mysql-statsd.conf

See our example [configuration](https://github.com/spilgames/mysql-statsd/blob/master/docs/mysql-statsd.conf)

Running the above command will start mysql_statsd in deamon mode. If you wish to see it's output, then run the command with -f / --foreground

## Usage

```
$ mysql_statsd --help
usage: mysql_statsd.py [-h] [-c FILE] [-d] [-f]

optional arguments:
  -h, --help            show this help message and exit
  -c FILE, --config FILE
                        Configuration file
  -d, --debug           Debug mode
  -f, --foreground      Dont fork main program
```

At the moment there is also a [deamon script](https://github.com/spilgames/mysql-statsd/blob/master/docs/mysql_statsd) for this package

You're more than welcome to help us improve it!

## Platforms

We would love to support many other kinds of database servers, but currently we're supporting these:

* MySQL 5.1
* MySQL 5.5
* Galera

Both MySQL versions supported with Percona flavour as well as vanilla.

### Todo:

Support for the following platforms

* Mysql 5.6
* MariaDB

We're looking forward to your pull request for other platforms

## Development installation 

To install package, setup a [python virtual environment](http://docs.python-guide.org/en/latest/dev/virtualenvs/)

Install the requirements(once the virtual environment is active):

``` 
pip install -r requirements.txt
```

*NOTE: MySQL-Python package needs mysql_config command to be in your path.*

*There are future plans to replace the mysql-python package with [PyMySQL](https://github.com/PyMySQL/PyMySQL)*

After that you're able to run the script through 

```
$ python mysql_statsd/mysql_statsd.py
```

## Coding standards

We like to stick with the python standard way of working: [PEP-8](http://legacy.python.org/dev/peps/pep-0008/)


Media:
========

Art gave a talk about this tool at Percona London 2013:
http://www.percona.com/live/mysql-conference-2013/sessions/mysql-performance-monitoring-using-statsd-and-graphite

Contributors
--------

spil-jasper

thijsdezoete

art-spilgames
