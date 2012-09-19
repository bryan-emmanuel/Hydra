#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#       untitled.py
#       
#       Copyright 2012 Bryan Emmanuel <bemmanuel@tardis>
#       
#       This program is free software; you can redistribute it and/or modify
#       it under the terms of the GNU General Public License as published by
#       the Free Software Foundation; either version 2 of the License, or
#       (at your option) any later version.
#       
#       This program is distributed in the hope that it will be useful,
#       but WITHOUT ANY WARRANTY; without even the implied warranty of
#       MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#       GNU General Public License for more details.
#       
#       You should have received a copy of the GNU General Public License
#       along with this program; if not, write to the Free Software
#       Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
#       MA 02110-1301, USA.

import socket
import json
import hashlib

def main():
	host = raw_input('Host:')
	port = raw_input('Port:')
	port = int(port)
	passphrase = raw_input('Passphrase:')
	
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.connect((host, port))
	data = json.loads(s.recv(1024))
	# get the salt and challenge
	salt = ''
	challenge = ''
	for obj in data.items():
		key = obj[0]
		value = obj[1]
		if (key == 'salt'):
			salt = value
		elif (key == 'challenge'):
			challenge = value
	
	action = raw_input('Action:')
	while (action != ''):
		authRequest = action
		target = ''
		columns = []
		values = []
		selection = ''
		statement = ''
		database = raw_input('Database:')
		if (database != ''):
			authRequest += database
			target = raw_input('Target:')
			if (target != ''):
				authRequest += target
				print 'Enter columns.'
				columns = []
				v = raw_input('column:')
				while (v != ''):
					if (v == '""'):
						v = ''
					authRequest += v
					columns.append(v)
					v = raw_input('column:')
				print 'Enter values.'
				values = []
				v = raw_input('value:')
				while (v != ''):
					if (v == '""'):
						v = ''
					authRequest += v
					values.append(v)
					v = raw_input('value:')
				selection = raw_input('selection:')
				authRequest += selection
				statement = raw_input('statement:')
				authRequest += statement
		print 'auth: ' + authRequest
		# add auth
		saltedPassphrase = hashlib.sha256(salt + passphrase).hexdigest()[:64]
		auth = hashlib.sha256(authRequest + challenge + saltedPassphrase).hexdigest()[:64]
		req = json.dumps({'action':action,'database':database,'target':target,'columns':columns,'values':values,'selection':selection,'statement':statement,'auth':auth})
		s.send(req + '\n')
		# read response
		errors = ''
		result = ''
		data = json.loads(s.recv(1024))
		for obj in data.items():
			key = obj[0]
			value = obj[1]
			if (key == 'challenge'):
				challenge = value
			elif (key == 'result'):
				result = value
			elif (key == 'errors'):
				errors = value
		print ''
		if (result != ''):
			print 'Result:'
			print ''
			for r in result:
				print r
		if (errors == ''):
			print ''
			action = raw_input('Action:')
		else:
			for error in errors:
				print error
			action = ''
	
	s.close()
	return 0

if __name__ == '__main__':
	main()

