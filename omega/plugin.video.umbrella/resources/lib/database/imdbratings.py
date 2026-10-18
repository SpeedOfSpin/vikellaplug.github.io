# -*- coding: utf-8 -*-
"""
	Umbrella Add-on
"""

import gzip
import os
from time import time
from sqlite3 import dbapi2 as db
from resources.lib.modules.control import existsPath, dataPath, makeFile, imdbRatingsFile, monitor
from resources.lib.modules import log_utils

dataset_url = 'https://datasets.imdbws.com/title.ratings.tsv.gz'
refresh_interval = 7 * 24 * 3600
query_chunk_size = 500 # stays under SQLite's 999 bound-parameter limit on older builds


class AbortRequested(Exception):
	pass


def needs_refresh():
	try: return time() - os.path.getmtime(imdbRatingsFile) > refresh_interval
	except OSError: return True

def refresh():
	import requests
	download_file = imdbRatingsFile + '.tsv.gz'
	build_file = imdbRatingsFile + '.tmp'
	try:
		if not existsPath(dataPath): makeFile(dataPath)
		with requests.get(dataset_url, stream=True, timeout=30) as response:
			response.raise_for_status()
			with open(download_file, 'wb') as file:
				for chunk in response.iter_content(chunk_size=65536): file.write(chunk)
		if os.path.exists(build_file): os.remove(build_file)
		dbcon = db.connect(build_file)
		try:
			dbcon.execute('''PRAGMA journal_mode = OFF''')
			dbcon.execute('''PRAGMA synchronous = OFF''')
			dbcon.execute('''CREATE TABLE ratings (id INTEGER PRIMARY KEY, rating TEXT)''')
			with gzip.open(download_file, 'rt', encoding='utf-8') as tsv:
				next(tsv) # header row
				dbcon.executemany('''INSERT INTO ratings VALUES (?, ?)''', _parse_rows(tsv))
			dbcon.commit()
		finally: dbcon.close()
		# Built in a separate file and swapped in so list views never read a half-built table.
		os.replace(build_file, imdbRatingsFile)
		return True
	except AbortRequested:
		return False
	except Exception:
		log_utils.error()
		return False
	finally:
		for path in (download_file, build_file):
			try: os.remove(path)
			except OSError: pass

def _parse_rows(tsv):
	for count, line in enumerate(tsv):
		# The full import takes a while on low-end devices, so let Kodi shut down mid-import.
		if count % 50000 == 0 and monitor.abortRequested(): raise AbortRequested()
		imdb, rating, _ = line.split('\t', 2)
		yield int(imdb[2:]), rating

def get_ratings(imdb_ids):
	"""Returns {imdb_id: rating} for the ids that IMDb has a rating for."""
	ids = {int(i[2:]): i for i in imdb_ids if i and i.startswith('tt') and i[2:].isdigit()}
	if not ids or not os.path.exists(imdbRatingsFile): return {}
	try:
		dbcon = db.connect(imdbRatingsFile, timeout=60)
		try:
			keys, rows = list(ids), []
			for start in range(0, len(keys), query_chunk_size):
				chunk = keys[start:start + query_chunk_size]
				rows += dbcon.execute('''SELECT id, rating FROM ratings WHERE id IN (%s)''' % ','.join('?' * len(chunk)), chunk).fetchall()
		finally: dbcon.close()
		return {ids[row[0]]: row[1] for row in rows}
	except Exception:
		log_utils.error()
		return {}
