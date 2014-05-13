#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
opsi python library - PgSQL

This module is part of the desktop management solution opsi
(open pc server integration) http://www.opsi.org

Copyright (C) 2013 uib GmbH

http://www.uib.de/

All rights reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License, version 3
as published by the Free Software Foundation.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Affero General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

@copyright: uib GmbH <info@uib.de>
@author: Jan Schneider <j.schneider@uib.de>
@author: Erol Ueluekmen <e.ueluekmen@uib.de>
@license: GNU Affero GPL version 3
"""

__version__ = '4.0.3.4'

import base64
import warnings
import time
import threading
from hashlib import md5

import psycopg2
import psycopg2.extras

from sqlalchemy import pool
from twisted.conch.ssh import keys

from OPSI.Logger import Logger
from OPSI.Types import BackendIOError, BackendBadValueError
from OPSI.Types import forceInt, forceUnicode
from OPSI.Backend.SQLpg import SQL, SQLBackend, SQLBackendObjectModificationTracker

logger = Logger()


class ConnectionPool(object):
	# Storage for the instance reference
	__instance = None

	def __init__(self, **kwargs):
		""" Create singleton instance """

		# Check whether we already have an instance
		if ConnectionPool.__instance is None:
			logger.info(u"Creating ConnectionPool instance")
			# Create and remember instance
			poolArgs = {}
			for key in ('pool_size', 'max_overflow', 'timeout'):
				if key in kwargs.keys():
					poolArgs[key] = kwargs[key]
					del kwargs[key]
			def creator():
				return psycopg2.connect(**kwargs)
			ConnectionPool.__instance = pool.QueuePool(creator, **poolArgs)
			con = ConnectionPool.__instance.connect()
			con.close()

		# Store instance reference as the only member in the handle
		self.__dict__['_ConnectionPool__instance'] = ConnectionPool.__instance

	def destroy(self):
		logger.notice(u"Destroying ConnectionPool instance")
		ConnectionPool.__instance = None

	def __getattr__(self, attr):
		""" Delegate access to implementation """
		return getattr(self.__instance, attr)

	def __setattr__(self, attr, value):
		""" Delegate access to implementation """
		return setattr(self.__instance, attr, value)


class PgSQL(SQL):

	AUTOINCREMENT = 'SERIAL'
	ALTER_TABLE_CHANGE_SUPPORTED = True
	ESCAPED_BACKSLASH  = "\\\\"
	ESCAPED_APOSTROPHE = "''"
	ESCAPED_ASTERISK   = "\\*"
	doCommit = True

	def __init__(self, **kwargs):

		self._address                   = u'localhost'
		self._username                  = u'opsi'
		self._password                  = u'opsi'
		self._database                  = u'opsi'
		self._databaseCharset           = 'utf8'
		self._connectionPoolSize        = 20
		self._connectionPoolMaxOverflow = 10
		self._connectionPoolTimeout     = 30

		# Parse arguments
		for (option, value) in kwargs.items():
			option = option.lower()
			if   option in ('address',):
				self._address = forceUnicode(value)
			elif option in ('username',):
				self._username = forceUnicode(value)
			elif option in ('password'):
				self._password = forceUnicode(value)
			elif option in ('database',):
				self._database = forceUnicode(value)
			elif option in ('databasecharset',):
				self._databaseCharset = str(value)
			elif option in ('connectionpoolsize',):
				self._connectionPoolSize = forceInt(value)
			elif option in ('connectionpoolmaxoverflow',):
				self._connectionPoolMaxOverflow = forceInt(value)
			elif option in ('connectionpooltimeout',):
				self._connectionPoolTimeout = forceInt(value)

		self._transactionLock = threading.Lock()
		self._pool = None

		self._createConnectionPool()
		logger.debug(u'PgSQL created: %s' % self)

	def _createConnectionPool(self):
		logger.debug2(u"Creating connection pool")
		self._transactionLock.acquire(0)
		try:
			try:
				if self._pool:
					self._pool.destroy()
				self._pool = ConnectionPool(
						host         = self._address,
						user         = self._username,
						password     = self._password,
						dbname       = self._database,
						pool_size    = self._connectionPoolSize,
						max_overflow = self._connectionPoolMaxOverflow,
						timeout      = self._connectionPoolTimeout,
				)

			except Exception as e:
				logger.logException(e)
				raise BackendIOError(u"Failed to connect to database '%s' address '%s': %s" % (self._database, self._address, e))
		finally:
			self._transactionLock.release()

	def connect(self):
		myConnectionSuccess = False
		myMaxRetryConnection = 10
		myRetryConnectionCounter = 0
		while (not myConnectionSuccess) and (myRetryConnectionCounter < myMaxRetryConnection):
			try:
				if (myRetryConnectionCounter > 0):
					self._createConnectionPool()
				logger.debug(u"Connecting to connection pool")
				self._transactionLock.acquire()
				logger.debug(u"Got thread lock")
				logger.debug(u"Connection pool status: %s" % self._pool.status())
				conn = self._pool.connect()
				cursor = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
				myConnectionSuccess = True
			except Exception as e:
				logger.debug(u"Execute error: %s" % e)
				if (e.args[0] == 2006):
					# 2006: 'PgSQL server has gone away'
					myConnectionSuccess = False
					if (myRetryConnectionCounter >= myMaxRetryConnection):
						logger.error(u'PgSQL server has gone away (Code 2006) - giving up after %d retries' % myRetryConnectionCounter)
						raise
					else:
						logger.notice(u'PgSQL server has gone away (Code 2006) - restarting Connection: retry %s' % myRetryConnectionCounter)
						myRetryConnectionCounter = myRetryConnectionCounter +1
						self._transactionLock.release()
						logger.debug(u"Thread lock released")
						time.sleep(0.1)
				else:
					logger.error(u'Unknown DB Error: %s' % str(e))
					self._transactionLock.release()
					raise
		return (conn, cursor)

	def close(self, conn, cursor):
		try:
			cursor.close()
			conn.close()
		finally:
			self._transactionLock.release()

	def getSet(self, query):
		(conn, cursor) = self.connect()
		valueSet = []
		try:
			try:
				self.execute(query, conn, cursor)
			except Exception as e:
				logger.debug(u"Execute error: %s" % e)
				if (e[0] != 2006):
					# 2006: PgSQL server has gone away
					raise
				self._createConnectionPool()
				(conn, cursor) = self.connect()
				self.execute(query, conn, cursor)
			valueSet = cursor.fetchall()

			if not valueSet:
				logger.debug(u"No result for query '%s'" % query)
				valueSet = []
		finally:
			self.close(conn, cursor)
		return valueSet

	def getRow(self, query, conn=None, cursor=None):
		closeConnection = True
		if conn and cursor:
			logger.debug(u"TRANSACTION: conn and cursor given, so we should not close the connection.")
			closeConnection = False
		else:
			(conn, cursor) = self.connect()
		row = {}
		try:
			try:
				self.execute(query, conn, cursor)
			except Exception as e:
				logger.debug(u"Execute error: %s" % e)
				if (e[0] != 2006):
					# 2006: PgSQL server has gone away
					raise
				self._createConnectionPool()
				(conn, cursor) = self.connect()
				self.execute(query, conn, cursor)
			row = cursor.fetchone()
			if not row:
				logger.debug(u"No result for query '%s'" % query)
				row = {}
			else:
				logger.debug2(u"Result: '%s'" % row)
		finally:
			if closeConnection:
				self.close(conn, cursor)
		return row

	def insert(self, table, valueHash, conn=None, cursor=None):

		closeConnection = True
		if conn and cursor:
			logger.debug(u"TRANSACTION: conn and cursor given, so we should not close the connection.")
			closeConnection = False
		else:
			(conn, cursor) = self.connect()
		result = -1
		try:
			colNames = values = u''
			for (key, value) in valueHash.items():
				colNames += u'"%s", ' % key
				if value is None:# or value == '':
					values += u"NULL, "
				elif type(value) is bool:
					if value:
						values += u"true, "
					else:
						values += u"false, "
				elif type(value) in (float, long, int):
					values += u"%s, " % value
				elif type(value) is str:
					values += u"\'%s\', " % (u'%s' % self.escapeApostrophe(self.escapeBackslash(value.decode("utf-8"))))
				else:
					values += u"\'%s\', " % (u'%s' % self.escapeApostrophe(self.escapeBackslash(value)))

			query = u'INSERT INTO "%s" (%s) VALUES (%s);' % (table, colNames[:-2], values[:-2])
			logger.debug2(u"insert: %s" % query)
			try:
				self.execute(query, conn, cursor)
			except Exception as e:
				logger.debug(u"Execute error: %s" % e)
				if (e[0] != 2006):
					# 2006: PgSQL server has gone away
					raise
				self._createConnectionPool()
				(conn, cursor) = self.connect()
				self.execute(query, conn, cursor)
			result = cursor.lastrowid
		finally:
			if closeConnection:
				self.close(conn, cursor)
		return result

	def update(self, table, where, valueHash, updateWhereNone=False):
		(conn, cursor) = self.connect()
		result = 0
		try:
			if not valueHash:
				raise BackendBadValueError(u"No values given")
			query = u'UPDATE "%s" SET ' % table
			for (key, value) in valueHash.items():
				if value is None and not updateWhereNone:
					continue
				query += u'"%s" = ' % key
				if value is None:
					query += u"NULL, "
				elif type(value) is bool:
					if value:
						query += u"true, "
					else:
						query += u"false, "
				elif type(value) in (float, long, int):
					query += u"%s, " % value
				elif type(value) is str:
					query += u"\'%s\', " % (u'%s' % self.escapeApostrophe(self.escapeBackslash(value.decode("utf-8"))))
				else:
					query += u"\'%s\', " % (u'%s' % self.escapeApostrophe(self.escapeBackslash(value)))

			query = u'%s WHERE %s;' % (query[:-2], where)
			logger.debug2(u"update: %s" % query)
			try:
				self.execute(query, conn, cursor)
			except Exception as e:
				logger.debug(u"Execute error: %s" % e)
				if (e[0] != 2006):
					# 2006: PgSQL server has gone away
					raise
				self._createConnectionPool()
				(conn, cursor) = self.connect()
				self.execute(query, conn, cursor)
			result = cursor.rowcount
		finally:
			self.close(conn, cursor)
		return result

	def delete(self, table, where, conn=None, cursor=None):
		closeConnection = True
		if conn and cursor:
			logger.debug(u"TRANSACTION: conn and cursor given, so we should not close the connection.")
			closeConnection = False
		else:
			(conn, cursor) = self.connect()
		result = 0
		try:
			query = u'DELETE FROM "%s" WHERE %s;' % (table, where)
			logger.debug2(u"delete: %s" % query)
			try:
				self.execute(query, conn, cursor)
			except Exception as e:
				logger.debug(u"Execute error: %s" % e)
				if (e[0] != 2006):
					# 2006: PgSQL server has gone away
					raise
				self._createConnectionPool()
				(conn, cursor) = self.connect()
				self.execute(query, conn, cursor)
			result = cursor.rowcount
		finally:
			if closeConnection:
				self.close(conn, cursor)
		return result

	def execute(self, query, conn=None, cursor=None):
		query = query.replace(' GROUP ',' "GROUP" ')

		res = None
		needClose = False
		if not conn or not cursor:
			(conn, cursor) = self.connect()
			needClose = True
		try:
			query = forceUnicode(query)
			logger.debug2(u"SQL query: %s" % query)
			res = cursor.execute(query)
			if self.doCommit:
				conn.commit()
		finally:
			if needClose:
				self.close(conn, cursor)
		return res

	def getTables(self):
		# Hardware audit database
		tables = {}
		logger.debug(u"Current tables:")
		for i in self.getSet(u"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"):
			tableName = i.values()[0].upper()
			logger.debug2(u" [ %s ]" % tableName)
			tables[tableName] = []
			for j in self.getSet(u"SELECT column_name FROM information_schema.columns WHERE table_name ='%s'" % tableName.upper()):
				logger.debug2(u"      %s" % j)
				tables[tableName].append(j['column_name'])
		return tables

	def getTableCreationOptions(self, table):
		return ''
#		if table in ('SOFTWARE', 'SOFTWARE_CONFIG') or table.startswith('HARDWARE_DEVICE_') or table.startswith('HARDWARE_CONFIG_'):
#			return u'ENGINE=MyISAM DEFAULT CHARSET utf8 COLLATE utf8_general_ci;'
#		return u'ENGINE=InnoDB DEFAULT CHARSET utf8 COLLATE utf8_general_ci'


class PgSQLBackend(SQLBackend):

	def __init__(self, **kwargs):
		self._name = 'pgsql'

		SQLBackend.__init__(self, **kwargs)
		self._sql = PgSQL(**kwargs)
		warnings.showwarning = self._showwarning

		self._licenseManagementEnabled = True
		self._licenseManagementModule = False
		self._sqlBackendModule = False

		modules = backendinfo['modules']
		helpermodules = backendinfo['realmodules']

		backendinfo = self._context.backend_info()
		logger.debug(u'PgSQLBackend created: %s' % self)

	def _showwarning(self, message, category, filename, lineno, line=None, file=None):
		#logger.warning(u"%s (file: %s, line: %s)" % (message, filename, lineno))
		if str(message).startswith('Data truncated for column'):
			logger.error(message)
		else:
			logger.warning(message)

	def _createTableHost(self):
		logger.debug(u'Creating table HOST')
		table = u'''CREATE TABLE "HOST" (
				"hostId" varchar(255) NOT NULL,
				"type" varchar(30),
				"description" varchar(100),
				"notes" varchar(500),
				"hardwareAddress" varchar(17),
				"ipAddress" varchar(15),
				"inventoryNumber" varchar(30),
				"created" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				"lastSeen" TIMESTAMP,
				"opsiHostKey" varchar(32),
				"oneTimePassword" varchar(32),
				"maxBandwidth" integer,
				"depotLocalUrl" varchar(128),
				"depotRemoteUrl" varchar(255),
				"depotWebdavUrl" varchar(255),
				"repositoryLocalUrl" varchar(128),
				"repositoryRemoteUrl" varchar(255),
				"networkAddress" varchar(31),
				"isMasterDepot" bool,
				"masterDepotId" varchar(255),
				PRIMARY KEY ("hostId")
			) %s;''' % self._sql.getTableCreationOptions('HOST')
		logger.debug(table)
		self._sql.execute(table)
		self._sql.execute('CREATE INDEX "index_host_type" on "HOST" ("type");')


class PgSQLBackendObjectModificationTracker(SQLBackendObjectModificationTracker):
	def __init__(self, **kwargs):
		SQLBackendObjectModificationTracker.__init__(self, **kwargs)
		self._sql = SQL(**kwargs)
		self._createTables()
