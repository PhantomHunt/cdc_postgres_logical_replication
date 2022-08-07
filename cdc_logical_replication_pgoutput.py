"""
Created on July 11 2022
@author: Vinjit
"""

import psycopg2
import time
from psycopg2.extras import LogicalReplicationConnection, StopReplication
from utilities import message_decoder, message_formatter

def main():                                                                                # Main code of CDC pgoutput
    conn = psycopg2.connect(
        database="mydatabasename",
        user='myusername', 
        password='mypassword',
        host='myhost',
        port='5432',
        connection_factory=LogicalReplicationConnection
    )
    cur = conn.cursor()
    options = {'publication_names': 'MyPublicationName', 'proto_version': '1'}

    try:
        cur.start_replication(slot_name='MySlotName', decode=False, options=options)
    except psycopg2.ProgrammingError:
        cur.create_replication_slot('MySlotName',output_plugin='pgoutput')
        cur.start_replication(slot_name='MySlotName', decode=False, options=options)

    class DemoConsumer(object):
        def __call__(self, msg):
            print("Original Encoded Message:\n",msg.payload)                                # Original encoded message
            message = message_decoder.decode_message(msg.payload)        
            print("\nOriginal Decoded Message:\n",message)                                  # Original decoded message
            formatted_message = message_formatter.get_message(str(message))
            print("\nFormatted Message:\n", formatted_message)                              # Decoded message formatted to JSON
            msg.cursor.send_feedback(flush_lsn=msg.data_start)
            print("\n#################### END OF MESSAGE #########################\n\n")
            # if (time.time() - start_time >= 80):                                          # Incase you want to stop replication 
            #     raise StopReplication()                                                   # after a certain duration of time

    # start_time = time.time()
    democonsumer = DemoConsumer()

    def start_stream():
        cur.consume_stream(democonsumer)
    try:       
        start_stream()
    except StopReplication: 
        print('\nStopping Replication')
    
if __name__ == '__main__':
    main()