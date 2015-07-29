===============================

Deprication
===========
This project is no longer supported by Spil Games and has been adopted by `DB-Art <https://github.com/db-art/>`_.

The new repository can be found here:
`MySQL-StatsD @ DB-Art <https://github.com/db-art/mysql-statsd>`_


mysql-statsd
===============================

Daemon that gathers statistics from MySQL and sends them to statsd.

-  Free software: BSD license
-  Documentation: http://mysql-statsd.rtfd.org.


Usage / Installation
====================

Install mysql\_statsd through pip(pip is a python package manager,
please don't use sudo!):

::

    pip install mysql_statsd

If all went well, you'll now have a new executable called mysql\_statsd
in your path.

Running mysql\_statsd
---------------------

::

    $ mysql_statsd --config /etc/mysql-statsd.conf 

Assuming you placed a config file in /etc/ named mysql-statsd.conf

See our example
`configuration <https://github.com/spilgames/mysql-statsd/blob/master/docs/mysql-statsd.conf>`__
or read below about how to configure

Running the above command will start mysql\_statsd in deamon mode. If
you wish to see it's output, then run the command with -f / --foreground


Usage
-----

::

    $ mysql_statsd --help
    usage: mysql_statsd.py [-h] [-c FILE] [-d] [-f]

    optional arguments:
      -h, --help            show this help message and exit
      -c FILE, --config FILE
                            Configuration file
      -d, --debug           Prints statsd metrics next to sending them
      --dry-run             Print the output that would be sent to statsd without
                            actually sending data somewhere
      -f, --foreground      Dont fork main program

At the moment there is also a `deamon
script <https://github.com/spilgames/mysql-statsd/blob/master/docs/mysql_statsd>`_
for this package

You're more than welcome to help us improve it!


Platforms
---------

We would love to support many other kinds of database servers, but
currently we're supporting these:

-  MySQL 5.1
-  MySQL 5.5
-  Galera

Both MySQL versions supported with Percona flavour as well as vanilla.

Todo:
~~~~~

Support for the following platforms

-  Mysql 5.6
-  MariaDB

We're looking forward to your pull request for other platforms

Development installation
------------------------

To install package, setup a `python virtual
environment <http://docs.python-guide.org/en/latest/dev/virtualenvs/>`_

Install the requirements(once the virtual environment is active):

::

    pip install -r requirements.txt

*NOTE: MySQL-Python package needs mysql\_config command to be in your
path.*

There are future plans to replace the mysql-python package with
`PyMySQL <https://github.com/PyMySQL/PyMySQL>`_

After that you're able to run the script through

::

    $ python mysql_statsd/mysql_statsd.py

Coding standards
----------------

We like to stick with the python standard way of working:
`PEP-8 <http://legacy.python.org/dev/peps/pep-0008/>`_



Configuration
=============

The configuration consists out of four sections:

-  daemon specific (log/pidfiles)
-  statsd (host, port, prefixes)
-  mysql (connecitons, queries, etc)
-  metrics (metrics to be stored including their type)

Daemon
------
The daemon section allows you to set the paths to your log and pic files

Statsd
------
The Statsd section allows you to configure the prefix and hostname of the 
metrics. In our example the prefix has been set to mysql and the hostname 
is included. This will log the status.com_select metric to:
mysql.<hostname>.status.com_select

You can use any prefix that is necessary in your environment.

MySQL
-----
The MySQL section allows you to configure the credentials of your mysql host
(preferrably on localhost) and the queries + timings for the metrics.
The queries and timings are configured through the stats_types configurable,
so take for instance following example:
::
    stats_types = status, innodb
This will execute both the query_status and query_innodb on the MySQL server.
The frequency can then be controlled through the time (in milliseconds) set in
the interval_status and interval_innodb.
The complete configuration would be:
::
    stats_types = status, innodb
    query_status = SHOW GLOBAL STATUS
    interval_status = 1000
    query_innodb = SHOW ENGINE INNODB STATUS
    interval_innodb = 10000

A special case is the query_commit: as the connection opened by mysql_statsd 
will be kept open and auto commit is turned off by default the status 
variables are not updated if your server is set to REPEATABLE_READ transaction 
isolation. Also most probably your history_list will skyrocket and your 
ibdata files will grow fast enough to drain all available diskspace. So when
in doubt about your transaction isolation: do include the query_commit!

Now here is the interesting part of mysql_statsd: if you wish to keep track 
of your own application data inside your application database you *could* 
create your own custom query this way. So for example:
::
    stats_types = myapp
    query_myapp = SELECT some_metric_name, some_metric_value FROM myapp.metric_table WHERE metric_ts >= DATE_SUB(NOW(), interval 1 MINUTE)
    interval_myapp = 60000

This will query your application database every 60 seconds, fetch all the 
metrics that have changed since then and send them through StatsD.
Obviously you need to whitelist them via the metrics section below.

Metrics
-------
The metrics section is basically a whitelisting of all metrics you wish to 
send to Graphite via StatsD. Currently there is no possibilty to whitelist all 
possible metrics, but there is a special case where we do allow wildcarding:
for the bufferpool\_* we whitelist all bufferpools with that specific metric.
Don't worry if you haven't configured multiple bufferpools: the output will 
be omitted by InnoDB and also not parsed by the preprocessor.

Important to know about the metrics is that you will have to specify what type 
they are. By default Graphite stores all metric equaly but treats them 
differently per type:

-  Gauge (g for gauge)
-  Rate (r for raw, d for delta)
-  Timer (t for timer)

Gauges are sticky values (like the spedometer in your car). Rates are the 
number of units that need to be translated to units per second. Timers are 
the time it took to perform a certain task.

An ever increasing value like the com\_select can be sent various ways. If you 
wish to retain the absolute value of the com_select it is advised to configure 
it as a gauge. However if you are going to use it as a rate (queries per 
second) it is no use storing it as a rate in the first place and then later 
on calculate the integral of the gauge to get the rate. It would be far more 
accurate to store it as a rate in the first place. 

Keep in mind that sending the com\_select value as a raw value is in this case 
a bad habit: StatsD will average out the collected metrics per second, so 
sending within a 10 second timeframe 10 times a value of 1,000,000 will average 
out to the expected 1,000,000. However as the processing of metrics also takes 
a bit of time the chance of missing one beat is relatively high and you end up
sending only 9 times the value, hence averaging out to 900,000 once in a while.

The best way to configure the com_select to a rate is by defining it as a delta.
The delta metric will remember the metric as it was during the previous run and 
will only send the difference of the two values.



Media:
======

Art gave a talk about this tool at Percona London 2013:
http://www.percona.com/live/mysql-conference-2013/sessions/mysql-performance-monitoring-using-statsd-and-graphite

Contributors
------------

spil-jasper

thijsdezoete

art-spilgames

bnkr
