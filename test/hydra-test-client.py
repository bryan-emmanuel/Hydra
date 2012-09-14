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
	
	protocol = raw_input('Protocol:')
	while (protocol != ''):
		req = protocol + '://'
		params = ''
		database = raw_input('Database:')
		if (database != ''):
			req += database + '/'
			obj = raw_input('Object:')
			if (obj != ''):
				req += obj
				params = raw_input('Parameters:')
				if (params != ''):
					req += '?' + params
		# add auth
		saltedPassphrase = hashlib.sha256(salt + passphrase).hexdigest()[:64]
		auth = hashlib.sha256(req + challenge + saltedPassphrase).hexdigest()[:64]
		print 'requestAuth: ' + req
		print 'challenge: ' + challenge
		print 'salt: ' + salt
		print 'passphrase: ' + passphrase
		print 'saltedpas: ' + saltedPassphrase
		print 'auth: ' + auth
		print 'len: %d' % len(auth)
		if (params == ''):
			req += '?auth='
		else:
			req += '&auth='
		req += auth
		print 'send: ' + req
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
			protocol = raw_input('Protocol:')
		else:
			for error in errors:
				print error
			protocol = ''
	
	s.close()
	return 0

if __name__ == '__main__':
	main()

