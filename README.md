# opsi-postgres
Postgres Backend for [OPSI](http://opsi.org)

* Under development
* [Bugs?](https://github.com/kochd/opsi-postgres/issues)
* Not tested in production
* Not official supported by UIB

If you are going to use/try this backend please send me a report to koch@triple6.org.
I need to know the bugs in order to fix them.

## How to install ?
### Setting up the database and user
First of all you have to setup a postgres database and create a user.

<pre>
apt-get install postgres sudo    # Install postgres and sudo
sudo -u postgres createuser opsi # create the user opsi
sudo -u postgres createdb opsi   # create the database opsi
sudo -u postgres psql            # open the psql cli
\password opsi                   # change password for user opsi
</pre>

### Copy files
* Copy Postgres.py and SQLpg.py to your pymodules destination ( e.g. /usr/lib/pymodules/python2.7/OPSI/Backend/ )
* Copy opsihwaudit.conf to /etc/opsi/hwaudit/
* Copy postgres.conf to /etc/opsi/backends/

### Configure
* Change pgsql.conf to match your database, user and password
* Change /etc/opsi/backendManager/dispatch.conf to your need ( e.g replace file or mysql by pgsql )

### Initialize
* Use opsi-setup --init-current-config to initial the change
* You can use opsi-convert to convert your old backend to pgsql ( e.g opsi-convert file pgsql or opsi-convert mysql pgsql )
* Restart your services ( opsiconfd , opsipxeconfd )




## Benchmarks
These benchmarks are based on the opsi-bench script also located in this repository.

* Create 500 Clients
* Get Hashes per Client
* Get List of Hashes
* Delete the 500 Clients


###Debian GNU/Linux 7 (Wheezy)
####mysql 5.5.37
|Mode|    Time    |
|----|------------|
|real|    1m1.760s|
|user|   0m19.649s|
|sys |    0m2.036s|

####postgres 9.1.13
|Mode|    Time    |
|----|------------|
|real|   0m35.908s|
|user|   0m15.837s|
|sys |    0m1.980s|

###Debian GNU/Linux 8 (Jessie)
####mysql 5.5.37
|Mode|    Time    |
|----|------------|
|real|   0m58.856s|
|user|   0m20.008s|
|sys |    0m2.200s|

####postgres 9.3.4
|Mode|    Time    |
|----|------------|
|real|   0m35.570s|
|user|   0m16.284s|
|sys |    0m2.168s|
