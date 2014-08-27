#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
opsi python library - Postgres

Copyright (C) 2014 Daniel Koch

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

@copyright: Daniel Koch <koch@triple6.org>
@author: Daniel Koch <koch@triple6.org>
@license: GNU Affero GPL version 3
"""

__version__ = '4.0.5.1'

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
from OPSI.Backend.Backend import ConfigDataBackend
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


class Postgres(SQL):

	AUTOINCREMENT = 'SERIAL'
	ALTER_TABLE_CHANGE_SUPPORTED = True
	ESCAPED_BACKSLASH  = "\\"
	ESCAPED_APOSTROPHE = "''"
	ESCAPED_ASTERISK   = "*"
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

	def connect(self, cursorType=None):
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

	def getRows(self, query):
		if not query.lower().startswith("select"):
			raise BackendIOError(u"getRows method allows select statements only, aborting.")
		logger.debug2(u"getRows: %s" % query)
		(conn, cursor) = self.connect(cursorType=MySQLdb.cursors.Cursor)
		valueSet = []
		try:
			try:
				self.execute(query, conn, cursor)
			except Exception, e:
				logger.debug(u"Execute error: %s" % e)
				if (e[0] != 2006):
					# 2006: MySQL server has gone away
					raise
				self._createConnectionPool()
				(conn, cursor) = self.connect(cursorType=MySQLdb.cursors.Cursor)
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
                        colNames = []
                        values = []
			for (key, value) in valueHash.items():
                                colNames.append(u'"{0}"'.format(key))
				if value is None:# or value == '':
					values.append(u"NULL")
				elif type(value) is bool:
					if value:
						values.append(u"true")
					else:
						values.append(u"false")
				elif type(value) in (float, long, int):
					values.append(u"{0}".format(value))
				elif type(value) is str:
					values.append(u"\'{0}\'".format(self.escapeApostrophe(self.escapeBackslash(value.decode("utf-8")))))
				else:
					values.append(u"\'{0}\'".format(self.escapeApostrophe(self.escapeBackslash(value))))

			query = u'INSERT INTO "{0}" ({1}) VALUES ({2});'.format(table, ', '.join(colNames), ', '.join(values))
			logger.debug2(u"insert: %s" % query)
			try:
				self.execute(query, conn, cursor)
			except psycopg2.DataError as de:
				logger.warning(de.message)
				logger.warning(u"Query: %s" % query)
				pass
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
                        query = []
			for (key, value) in valueHash.items():
				if value is None:
					if not updateWhereNone:
						continue

					value = u"NULL"
				elif type(value) is bool:
					if value:
						value = u"true"
					else:
						value = u"false"
				elif type(value) in (float, long, int):
					value = u"%s" % value
				elif type(value) is str:
					value = u"\'{0}\'".format(self.escapeApostrophe(self.escapeBackslash(value.decode("utf-8"))))
				else:
					value = u"\'{0}\'".format(self.escapeApostrophe(self.escapeBackslash(value)))

				query.append(u'"{0}" = {1}'.format(key, value))
			query = u'UPDATE "{0}" SET {1} WHERE {2};'.format(table, ', '.join(query), where)

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


class PostgresBackend(SQLBackend):

	def __init__(self, **kwargs):
		self._name = 'pgsql'

		SQLBackend.__init__(self, **kwargs)
		self._sql = Postgres(**kwargs)
		warnings.showwarning = self._showwarning

		self._licenseManagementEnabled = True
		self._licenseManagementModule = False
		self._sqlBackendModule = False

		backendinfo = self._context.backend_info()
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

	# Overwriting productProperty_insertObject and
	# productProperty_updateObject to implement Transaction
	def productProperty_insertObject(self, productProperty):
		ConfigDataBackend.productProperty_insertObject(self, productProperty)
		data = self._objectToDatabaseHash(productProperty)
		possibleValues = data['possibleValues']
		defaultValues = data['defaultValues']
		if possibleValues is None:
			possibleValues = []
		if defaultValues is None:
			defaultValues = []
		del data['possibleValues']
		del data['defaultValues']

		where = self._uniqueCondition(productProperty)
		if self._sql.getRow('select * from "PRODUCT_PROPERTY" where %s' % where):
			self._sql.update("PRODUCT_PROPERTY", where, data, updateWhereNone = True)
		else:
			self._sql.insert("PRODUCT_PROPERTY", data)

		if not possibleValues is None:
			(conn, cursor) = self._sql.connect()
			myTransactionSuccess = False
			myMaxRetryTransaction = 10
			myRetryTransactionCounter = 0
			while (not myTransactionSuccess) and (myRetryTransactionCounter < myMaxRetryTransaction):
				try:
					myRetryTransactionCounter += 1
					# transaction
					cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")
					self._sql.doCommit = False
#					conn.begin()
					logger.notice(u'Start Transaction: delete from ppv %d' % myRetryTransactionCounter)

					self._sql.delete('PRODUCT_PROPERTY_VALUE', where, conn, cursor)
					conn.commit()
					myTransactionSuccess = True
				except Exception as e:
					logger.debug(u"Execute error: %s" % e)
					if (e.args[0] == 1213):
						# 1213: 'Deadlock found when trying to get lock; try restarting transaction'
						# 1213: May be table locked because of concurrent access - retrying
						myTransactionSuccess = False
						if (myRetryTransactionCounter >= myMaxRetryTransaction):
							logger.error(u'Table locked (Code 2013) - giving up after %d retries' % myRetryTransactionCounter)
							raise
						else:
							logger.notice(u'Table locked (Code 2013) - restarting Transaction')
							time.sleep(0.1)
					else:
						logger.error(u'Unknown DB Error: %s' % str(e))
						raise

				logger.notice(u'End Transaction')
				self._sql.doCommit = True
				logger.notice(u'doCommit set to true')
			self._sql.close(conn,cursor)

		(conn, cursor) = self._sql.connect()
		for value in possibleValues:
			try:
				# transform arguments for sql
				# from uniqueCondition
				if (value in defaultValues):
					myPPVdefault = u'"isDefault" = \'true\''
				else:
					myPPVdefault = u'"isDefault" = \'false\''

				if type(value) is bool:
					if value:
						myPPVvalue = u'"value" = \'true\''
					else:
						myPPVvalue = u'"value" = \'false\''
				elif type(value) in (float, long, int):
					myPPVvalue = u'"value" = %s' % (value)
				else:
					myPPVvalue = u"\"value\" = '%s'" % (self._sql.escapeApostrophe(self._sql.escapeBackslash(value)))
				myPPVselect = (
					u"select * from \"PRODUCT_PROPERTY_VALUE\" where "
					u"\"propertyId\" = '{0}' AND \"productId\" = '{1}' AND "
					u"\"productVersion\" = '{2}' AND "
					u"\"packageVersion\" = '{3}' AND {4} AND {5}".format(
						data['propertyId'],
						data['productId'],
						str(data['productVersion']),
						str(data['packageVersion']),
						myPPVvalue,
						myPPVdefault
					)
				)
				myTransactionSuccess = False
				myMaxRetryTransaction = 10
				myRetryTransactionCounter = 0
				while (not myTransactionSuccess) and (myRetryTransactionCounter < myMaxRetryTransaction):
					try:
						myRetryTransactionCounter += 1
						# transaction
						cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")
						self._sql.doCommit = False
#						conn.begin()
						logger.notice(u'Start Transaction: insert to ppv %d' % myRetryTransactionCounter)
						if not self._sql.getRow(myPPVselect , conn, cursor):
							# self._sql.doCommit = True
							logger.notice(u'doCommit set to true')
							self._sql.insert('PRODUCT_PROPERTY_VALUE', {
								'productId': data['productId'],
								'productVersion': data['productVersion'],
								'packageVersion': data['packageVersion'],
								'propertyId': data['propertyId'],
								'value': value,
								'isDefault': (value in defaultValues)
								}, conn, cursor)
							conn.commit()
						else:
							conn.rollback()
						myTransactionSuccess = True
					except Exception as e:
						logger.debug(u"Execute error: %s" % e)
						if (e.args[0] == 1213):
							# 1213: 'Deadlock found when trying to get lock; try restarting transaction'
							# 1213: May be table locked because of concurrent access - retrying
							myTransactionSuccess = False
							if (myRetryTransactionCounter >= myMaxRetryTransaction):
								logger.error(u'Table locked (Code 2013) - giving up after %d retries' % myRetryTransactionCounter)
								raise
							else:
								logger.notice(u'Table locked (Code 2013) - restarting Transaction')
								time.sleep(0.1)
						else:
							logger.error(u'Unknown DB Error: %s' % str(e))
							raise

				logger.notice(u'End Transaction')
			finally:
				self._sql.doCommit = True
				logger.notice(u'doCommit set to true')
		self._sql.close(conn,cursor)

		def productProperty_updateObject(self, productProperty):
			if not self._sqlBackendModule:
				raise Exception(u"SQL backend module disabled")

			ConfigDataBackend.productProperty_updateObject(self, productProperty)
			data = self._objectToDatabaseHash(productProperty)
			where = self._uniqueCondition(productProperty)
			possibleValues = data['possibleValues']
			defaultValues = data['defaultValues']
			if possibleValues is None:
				possibleValues = []
			if defaultValues is None:
				defaultValues = []
			del data['possibleValues']
			del data['defaultValues']
			self._sql.update('PRODUCT_PROPERTY', where, data)

			if not possibleValues is None:
				self._sql.delete('PRODUCT_PROPERTY_VALUE', where)

			for value in possibleValues:
				try:
					self._sql.doCommit = False
					logger.notice(u'doCommit set to false')
					valuesExist = self._sql.getRow(
						u"select * from \"PRODUCT_PROPERTY_VALUE\" where "
						u"\"propertyId\" = '{0}' AND \"productId\" = '{1}' AND "
						u"\"productVersion\" = '{2}' AND \"packageVersion\" = '{3}' "
						u"AND \"value\" = '{4}' AND \"isDefault\" = {5}".format(
							data['propertyId'],
							data['productId'],
							str(data['productVersion']),
							str(data['packageVersion']),
							value,
							str(value in defaultValues)
						)
					)
					if not valuesExist:
						self._sql.doCommit = True
						logger.notice(u'doCommit set to true')
						self._sql.insert('PRODUCT_PROPERTY_VALUE', {
							'productId': data['productId'],
							'productVersion': data['productVersion'],
							'packageVersion': data['packageVersion'],
							'propertyId': data['propertyId'],
							'value': value,
							'isDefault': (value in defaultValues)
							}
						)
				finally:
					self._sql.doCommit = True
					logger.notice(u'doCommit set to true')



class PostgresBackendObjectModificationTracker(SQLBackendObjectModificationTracker):
	def __init__(self, **kwargs):
		SQLBackendObjectModificationTracker.__init__(self, **kwargs)
		self._sql = SQL(**kwargs)
		self._createTables()
