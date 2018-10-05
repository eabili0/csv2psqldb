
#csv2db.py
import psycopg2
import sys
import os
import Util
from unicodeMagic import UnicodeReader

class Database:
	def __init__(self, db, dbUser, dbHost, dbPort, password):
		self.conn = psycopg2.connect(database=db, user=dbUser, host=dbHost, port=dbPort, password=password)
		self.cur = self.conn.cursor()

def getInsertCommand(first_row, table_name):
	values = '(%s'
	fields = '('+first_row[0]

	for x in range(len(first_row)-1):
		values = values + ',%s'
		fields = fields + ',' + first_row[x+1]
	values = values + ')'
	fields = fields + ')'

	# return "INSERT into assert." + table_name + " " + fields + " VALUES " + values
	return (fields, values)

def toDB(file_path, server_name, db_name, table_name, user, password, csv_delimiter):

	folder, file = os.path.split(file_path)
	with Util.cd(folder):
		reader = UnicodeReader(open(file,'rb'))
		db = Database(db_name, user, server_name, 5432, password)
		csv_delimiter = csv_delimiter if csv_delimiter != "" else ";"
		fields, values = getInsertCommand(reader.next()[0].split(csv_delimiter), table_name)

		print 'running...'
		v = []
		while True:
			try:
				row = reader.next()[0].split(',')
			except Exception:
				break;

			for i in range(len(row)):
				if row[i] == 'None':
					row[i] = ''

			v.append(tuple(row))
			
		args_str = ','.join(db.cur.mogrify(values, x) for x in tuple(v))
		db.cur.execute("INSERT INTO " + table_name + " " + fields + " VALUES " + args_str)
		# db.cur.executemany(insert_command, tuple(v))
		db.conn.commit()

		db.conn.close()
	
	print 'done!'

def main():

  if len(sys.argv) < 7:
    print "wrong format\ncsv2db.py file_path server_name db_name table_name user password delimiter"
    sys.exit()

  if not os.path.isfile(sys.argv[1]):
    print("!! %s not a valid file" % sys.argv[1])
    sys.exit()

  toDB(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7])

if __name__ == "__main__":
	main()

















































