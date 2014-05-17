# opsi-postgres
Postgres Backend for [OPSI](http://opsi.org)

* Under development
* [Bugs?](./issues)
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
* Copy PgSQL.py and SQLpg.py to your pymodules destination ( e.g. /usr/lib/pymodules/python2.7/OPSI/Backend/ )
* Copy opsihwaudit.conf to /etc/opsi/hwaudit/
* Copy pgsql.conf to /etc/opsi/backends/

### Configure
* Change pgsql.conf to match your database, user and password
* Chnage /etc/opsi/backendManager/dispatch.conf to your need ( e.g replace file or mysql by pgsql )

### Initialize
* Use opsi-setup --init-current-config to initial the change
* You can use opsi-convert to convert your old backend to pgsql ( e.g opsi-convert file pgsql or opsi-convert mysql pgsql )
* Restart your services ( opsiconfd , opsipxeconfd )
