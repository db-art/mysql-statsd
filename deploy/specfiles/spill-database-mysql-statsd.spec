%define         my_app_name     mysql-statsd
%define         _description    Daemon that gathers statistics from MySQL and sends them to statsd.

Summary:        ${_description}

Requires: libssl.so.6
BuildRequires: openssl-devel python27-distribute python-virtualenv python27 Percona-Server-client-56 Percona-Server-shared-56 Percona-Server-devel-56

# Filled by DOS template engine
%define     my_app_ver      {{VER}}
%define     my_app_rel      {{SPI}}

# set command used to create virtualenv
%define     virtualenv      /usr/bin/virtualenv --python=python2.7

# set to 1 to enable automatic django syncdb/migrate
%define     django_dbupdate     0

# set to 1 to enable creation of a service user
%define     create_user          1

# https://github.com/spilgames/specfiles/blob/master/templates/spil-libs-python-project-v3.inc
%include %{_topdir}/SOURCES/spil-libs-python-project-v3.inc
