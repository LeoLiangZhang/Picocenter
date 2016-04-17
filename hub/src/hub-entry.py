import MySQLdb
from PicoManager import PicoManager
from boto.s3.connection import S3Connection
from boto.s3.key import Key

args = ['path-to-executable', 'hostname', 'ports', 'customer_id']
if len(sys.argv) < len(args)+1:
    sys.exit('usage: python hub-entry.py ' + ' '.join(['['+arg+']' for arg in args]))

conn = S3Connection('<aws access key>', '<aws secret key>')
bucket = aws_connection.get_bucket('pico')

db = MySQLdb.connect(host='localhost',user='root',passwd='root',db='picocenter')
db.autocommit(True)

picomanager = PicoManager(db, None)
pico_id = picomanager.new_picoprocess(args[2], args[3], args[4])

k = Key(bucket)
fname = args[1].split('/')[-1]
k.key = 'pico/{0}/files/{1}'.format(pico_id, fname)
k.set_contents_from_filename(args[1])

cursor.close()