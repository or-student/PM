
import pika
import json

def json_message_info(country_name,year,music_type,route = r"C:\Users\OR-PC\Desktop\for_PM\chinook.db"):
    m_info = {"route":route,"country_name":country_name,"year":year,"music_type":music_type}
    return json.dumps(m_info)

new_message = json_message_info(country_name = "Germany",year = 2009,music_type = "Rock")

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))

channel = connection.channel()

channel.exchange_declare(exchange='logs', exchange_type='fanout')

channel.basic_publish(exchange='logs', routing_key='', body=new_message)
print(" [x] Sent %r" % new_message)

connection.close()