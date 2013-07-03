import sqlite3
import time

start_time = time.time()

message_to_field = ["ROWID","guid","text","replace","service_center","handle_id","subject","country","attributedBody","version","type","service","account","account_guid","error","date","date_read","date_delivered","is_delivered","is_finished","is_emote","is_from_me","is_empty","is_delayed","is_auto_reply","is_prepared","is_read","is_system_message","is_sent","has_dd_results","is_service_message","is_forward","was_downgraded","is_archive","cache_has_attachments","cache_roomnames","was_data_detected","was_deduplicate"]

def getMessages():
	conn = sqlite3.connect('texts.db')
	c = conn.cursor()
	c.execute('select * from message')
	messages = []
	message_id_to_message = {}
	messageTuple = c.fetchone()
	while messageTuple:
		message = messageToField(messageTuple)
		message_id_to_message[message["ROWID"]] = message
		messages.append(message)
		messageTuple = c.fetchone()	
	c.execute('select * from chat_message_join')
	tuple_ = c.fetchone()
	while tuple_:
		(chat_id, message_id) = tuple_
		message = message_id_to_message[message_id]
		message["chat_id"] = chat_id
		tuple_ = c.fetchone()
	for message in messages:
		if "chat_id" in message.keys():
			c.execute('select ROWID, chat_identifier from chat where ROWID=%s' % message["chat_id"])
			message["number"] = c.fetchone()[1].lstrip('+')
		else:
			message["chat_id"] = message["number"] = ''
	conn.commit()
	c.close()
	return messages

	
# put in a message tuple and get back a dictionary with field names
def messageToField(messageTuple):
	message = {}
	for i in range(len(message_to_field)):
		message[message_to_field[i]] = messageTuple[i] if (messageTuple[i] != None) else ''
	return message

messages = getMessages()

import unicodedata
def st(field):
	if field != None:
		if type(field) == type(u"string"):
			return unicodedata.normalize('NFKD', field).encode('ascii','ignore')
		return str(field)
	return ""

import string
words = {}
for message in messages:
	body = message['text']
	body = st(body).lower().replace("\n", " ").replace("\r", " ")
	body = body.translate(string.maketrans("",""), string.punctuation)
	words_in_body = body.split(" ")
	for word in words_in_body:
		if word in words:
			words[word] += 1
		else: words[word] = 1
dates = map(lambda x: x['date'], messages)

import csv
contacts_list = []
contacts_file = csv.DictReader(open('contacts.csv'))
for row in contacts_file:
	contacts_list.append(row)
num_to_name = {}
phone_fields = ['Primary Phone', 'Home Phone', 'Home Phone 2', 'Mobile Phone', 'Pager', 'Company Main Phone', 'Business Phone', 'Business Phone 2']
for contact in contacts_list:
	for field in phone_fields:
		if len(contact[field]) and (contact['First Name'].strip(' ') or contact['Last Name'].strip(' ')):
			phone_num = ''.join(filter(lambda x: x in string.digits, list(contact[field]))) 
			num_to_name[phone_num.lstrip('1')] = contact['First Name'] + " " + contact['Last Name']
import pdb
def createTable():
	conn = sqlite3.connect('texts.db')
	c = conn.cursor()
	c.execute('DROP TABLE message_info')
	create_table = "CREATE TABLE message_info("
	create_table+= "ROWID INT,"
	create_table+= "text TEXT,"
	create_table+= "is_from_me INT,"
	create_table+= "date INT,"
	create_table+= "date_read INT,"
	create_table+= "date_delivered INT,"
	create_table+= "number TEXT,"
	create_table+= "name TEXT)"
	print create_table
	c.execute(create_table)
	for message in messages:
		insert_message = "INSERT INTO message_info values("
		insert_message += st(message["ROWID"]) + ","
		insert_message += "'" + st(message["text"]).strip().replace("'","''") + "',"
		insert_message += st(message["is_from_me"]) + ","
		insert_message += st(message["date"]) + ","
		insert_message += st(message["date_read"]) + ","
		insert_message += st(message["date_delivered"]) + ",'"
		insert_message += st(message["number"].lstrip('1')) + "','"
		insert_message += st(num_to_name.get(message["number"].lstrip('1'), "").replace("'","''")) + "'"
		insert_message += ")"
		print insert_message
		c.execute(insert_message)
	conn.commit()
	c.close()

createTable()

end_time = time.time()
print "Analysis took " + str(end_time-start_time) + " seconds"
