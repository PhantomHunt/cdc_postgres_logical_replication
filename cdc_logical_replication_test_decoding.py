"""
Created on July 4 2022
@author:Vinjit
"""

import psycopg2
from psycopg2.extras import LogicalReplicationConnection

conn = psycopg2.connect(
    host='myhost',
    dbname='mydatabasename',
    user='myusername',
    password='mypassword',
    port='5432', 
    connection_factory=LogicalReplicationConnection
    )

cur = conn.cursor()
class DemoConsumer(object):
    def __call__(self, msg):        
        print("\nOriginal Decoded Message:\n",msg.payload)
        msg.cursor.send_feedback(flush_lsn=msg.data_start)
        print("\n#################### END OF MESSAGE #######################\n\n")

try:
    cur.start_replication(slot_name='MyPublicationName', decode=True)
except psycopg2.ProgrammingError:
    cur.create_replication_slot('MyPublicationName', output_plugin='test_decoding')
    cur.start_replication(slot_name='MyPublicationName', decode=True)

democonsumer = DemoConsumer()
cur.consume_stream(democonsumer)

