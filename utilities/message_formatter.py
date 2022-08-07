"""
Created on July 12 2022
@author: Vinjit
"""

import re

relation_pool={} 
trans_info = [None] * 3

def update_timestamp(splitted_records):
        trans_info[0]=(splitted_records[3].split(' : ')[1]).strip()     # timestamp
        trans_info[1]=(splitted_records[2].split(':')[1]).strip()       # lsn
        trans_info[2]=(splitted_records[4].split(':')[1]).strip()       # xid

def replace_relation(splitted_records,relation_id,relationdata):
        new_schema_name = ((splitted_records[3].split(':')[1]).strip()).strip("'")
        # print (new_schema_name)

        new_table_name = ((splitted_records[4].split(':')[1]).strip()).strip("'")
        # print (new_table_name)

        new_col_list = []
        new_col_record = relationdata.split(':')[-1]
        # print('col rec : ', new_col_record)
        new_col_list=re.findall(r'\'.*?\'', new_col_record)
        for i in range(len(new_col_list)):
                new_col_list[i]=new_col_list[i].strip("'")

        relation_pool[relation_id] = {
		                "Namespace/schema" : new_schema_name,
		                "Table_name" : new_table_name,
		                "Columns" : new_col_list
	                }
        
        # print("\nThe current relation pool is : ",relation_pool)
        

def add_relation(splitted_records,relation_id,relationdata):
        schema_name = ((splitted_records[3].split(':')[1]).strip()).strip("'")
        # print (schema_name)

        table_name = ((splitted_records[4].split(':')[1]).strip()).strip("'")
        # print (table_name)

        col_list = []
        col_record = relationdata.split(':')[-1]
        # print('col rec : ',col_record)
        col_list=re.findall(r'\'.*?\'',col_record)
        for i in range(len(col_list)):
                col_list[i]=col_list[i].strip("'")

        relation_dict = {
		                "Namespace/schema" : schema_name,
		                "Table_name" : table_name,
		                "Columns" : col_list
	                }
        # print(relation_dict)

        relation_pool.update({relation_id : relation_dict})
        # print("\nThe current relation pool is : ",relation_pool)

def update_relation_pool(splitted_records,relationdata):
        relation_id=(splitted_records[2].split(':')[1]).strip()
        # print("relation id is: ",relation_id)
        if relation_id in relation_pool:                
                # print("rel found")
                replace_relation(splitted_records,relation_id,relationdata)
                # print(f"\nSUCCESS : Relation {relation_id} updated in the relation pool")
                        
        else:
                # print("rel not found")
                add_relation(splitted_records,relation_id,relationdata)
                # print(f"\nSUCCESS : New Relation {relation_id} added to the relation pool")

def create_insert_message_json(splitted_records,insertdata):
        relation_id = (splitted_records[2].split(':')[1]).strip()
        if relation_id in relation_pool:
                rels=relation_pool[relation_id]
                col_names=rels['Columns']
                schema_name=rels['Namespace/schema']
                table_name=rels['Table_name']                       

        else:
                # print(f"\nERROR : Relation id {relation_id} not found in pool")
                raise Exception (f"\nERROR : Relation id {relation_id} not found in pool")          

        raw_cols = (insertdata.split(':')[-1]).split(',')
        # print(raw_cols)
        col_data_string = ""
        for i in range(2,len(raw_cols),3):
                col_data_string+=raw_cols[i]
        col_data=re.findall(r'\'.*?\'', col_data_string)

        for i in range(len(col_data)):
                col_data[i]=col_data[i].strip("'")
        # print(col_data)

        if(len(col_data) != len(col_names)):
                # print(f"\nERROR : Number of columns inserted is not equal to number of columns in relation {relation_id}")
                raise Exception (f"\nERROR : Number of columns inserted is not equal to number of columns in relation {relation_id}")
        
        insert_message = {
                "Operation" : "INSERT",
                "LSN" : trans_info[1], 
                "Transaction_Xid" : trans_info[2],
                "Commit_timestamp" : trans_info[0],   
                "Schema" : schema_name,
                "Table_name" : table_name,
                "Relation_id" : relation_id
        }

        insert_message.update(zip(col_names, col_data))
        return insert_message

def create_update_message_json(splitted_records,updatedata):
        relation_id = (splitted_records[2].split(':')[1]).strip()
        if relation_id in relation_pool:
                rels=relation_pool[relation_id]
                col_names=rels['Columns']
                schema_name=rels['Namespace/schema']
                table_name=rels['Table_name']                       

        else:
                # print(f"\nERROR : Relation id {relation_id} not found in pool")
                raise Exception (f"\nERROR : Relation id {relation_id} not found in pool")

        raw_cols = (updatedata.split(':')[-1]).split(',')
        # print(raw_cols)
        col_data_string = ""
        for i in range(2,len(raw_cols),3):
                col_data_string+=raw_cols[i]
        col_data=re.findall(r'\'.*?\'', col_data_string)

        for i in range(len(col_data)):
                col_data[i]=col_data[i].strip("'")
        # print(col_data)

        if(len(col_data) != len(col_names)):
                # print(f"\nERROR : Number of columns inserted is not equal to number of columns in relation {relation_id}")
                raise Exception (f"\nERROR : Number of columns inserted is not equal to number of columns in relation {relation_id}")
        
        update_message = {
                "Operation" : "UPDATE",
                "LSN" : trans_info[1], 
                "Transaction_Xid" : trans_info[2],
                "Commit_timestamp" : trans_info[0],
                "Schema" : schema_name,
                "Table_name" : table_name,
                "Relation_id" : relation_id
        }

        update_message.update(zip(col_names, col_data))
        return update_message

def create_truncate_message_json(truncatedata):
        
        raw_rel_ids = ((truncatedata.split(':')[-1].strip()).lstrip('[')).rstrip(']')
        # print("new rec - ",raw_rel_ids)
        relation_ids=raw_rel_ids.split(',')
        relation_ids=[ x.strip() for x in relation_ids]
        table_list = []
        schema_list = []

        for ids in relation_ids:
                if ids in relation_pool:
                        rels=relation_pool[ids]
                        schema_list.append(rels['Namespace/schema'])
                        table_list.append(rels['Table_name'])
                                

        truncate_message = {
                "Operation" : "TRUNCATE",
                "LSN" : trans_info[1], 
                "Transaction_Xid" : trans_info[2],
                "Commit_timestamp" : trans_info[0],
                "Schema_names" : schema_list,
                "Table_names" : table_list,
                "Relation_ids" : relation_ids
        }
        return truncate_message

def create_delete_message_json(splitted_records,deletedata):
        relation_id = (splitted_records[2].split(':')[1]).strip()
        if relation_id in relation_pool:
                rels=relation_pool[relation_id]
                col_names=rels['Columns']
                schema_name=rels['Namespace/schema']
                table_name=rels['Table_name']                       

        else:
                # print(f"\nERROR : Relation id {relation_id} not found in pool")
                raise Exception (f"\nERROR : Relation id {relation_id} not found in pool")
        
        raw_col = (deletedata.split(':')[-1]).split(',')
        result = re.search('col_data=(.*)', raw_col[2])
        # print(result)
        result=result.group(1)
        col_data=(result.strip(')')).strip("'")

        delete_message = {
                "Operation" : "DELETE",
                "LSN" : trans_info[1], 
                "Transaction_Xid" : trans_info[2],
                "Commit_timestamp" : trans_info[0],
                "Schema" : schema_name,
                "Table_name" : table_name,
                "Relation_id" : relation_id,
                col_names[0] : col_data
        }
        delete_message.update()
        return delete_message

def get_message(data):
        try:
                splitted_records=data.split(',')
                operation=(splitted_records[0].split(':')[1]).strip()
                if(operation=='RELATION'):
                        update_relation_pool(splitted_records,data)
                        return ''
                elif(operation=='BEGIN'):
                        print("in begin")
                        update_timestamp(splitted_records)
                        return ''
                elif(operation=='INSERT'):
                        message=create_insert_message_json(splitted_records,data)
                        return message
                elif(operation=='UPDATE'):
                        message=create_update_message_json(splitted_records,data)
                        return message
                elif(operation=='DELETE'):
                        message=create_delete_message_json(splitted_records,data)
                        return message
                elif(operation=='TRUNCATE'):
                        message=create_truncate_message_json(data)
                        return message
                else:
                        return ''
        except Exception as error:
                # print(error)
                return error
       
# def main(data):        
#         message=get_message(data)
#         if(message != ''):
#                 print(message)