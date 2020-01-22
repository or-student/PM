#!/usr/bin/env python

import pika
import json
import pandas as pd
import sqlite3

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))

channel = connection.channel()

channel.exchange_declare(exchange='logs', exchange_type='fanout')

result = channel.queue_declare(queue='', exclusive=True)

queue_name = result.method.queue

channel.queue_bind(exchange='logs', queue=queue_name)

print(' [*] Waiting for logs. To exit press CTRL+C')

def create_table(conn,create_table_for_sql):
    c = conn.cursor()
    c.execute(create_table_for_sql)



def callback(ch, method, properties, body):   
    print(" [x] %r" %  json.loads(body))
    json_load = json.loads(body)    
    db_connection = sqlite3.connect(json_load['route'])
    y = json_load['year']
    c = json_load['country_name']
    m = json_load['music_type']
    df2_1 = pd.read_sql_query("SELECT billingCountry,count(*) as purchases FROM invoices i group by billingCountry" ,db_connection)
    df2_1.to_csv(r"C:\Users\OR-PC\Desktop\for_PM\purchases.csv")
    
    df2_2 = pd.read_sql_query("SELECT i2.BillingCountry,sum(i1.Quantity) as number_of_items FROM "+
                              "invoice_items i1 inner join invoices i2 on i1.InvoiceId = i2.InvoiceId"+
                              " group by i2.BillingCountry",db_connection)
    df2_2.to_csv(r"C:\Users\OR-PC\Desktop\for_PM\items.csv")

    df2_3 = pd.read_sql_query("select BillingCountry,album_title from "+ 
                              "(select a.AlbumId,a.Title as album_title,t.TrackId,t.Name as song_name,t.GenreId,g.Name as genre ,ii.InvoiceId,i.BillingCountry "+  
                              "from tracks t inner join albums a on t.albumId = a.albumId "+
                              "inner join genres g on g.GenreId=t.GenreId inner join invoice_items ii on ii.TrackId = t.TrackId "+
                              "inner join invoices i on i.InvoiceId = ii.InvoiceId) group by BillingCountry,album_title" ,db_connection)
    df2_3 = df2_3.to_json(orient = 'table')
    
    
    df2_4 = pd.read_sql("select album_title,BillingCountry,count(*) as amount_of_song_being_sold from( "+
                          "select a.AlbumId,a.Title as album_title,t.TrackId,t.Name as song_name,t.GenreId,g.Name as genre ,ii.InvoiceId,i.BillingCountry,i.InvoiceDate from "+ 
                          "tracks t inner join albums a on t.albumId = a.albumId inner join genres g on g.GenreId=t.GenreId "+
                          "inner join invoice_items ii on ii.TrackId = t.TrackId inner join invoices i on i.InvoiceId = ii.InvoiceId "+
                          "where i.BillingCountry = %r and genre = %r and i.InvoiceDate between '%d-01-01' and '%d-12-31') " % (c,m,y,y)+
                          "group by album_title,BillingCountry,genre "+
                          "having amount_of_song_being_sold = (select max(amount_of_song_being_sold) from (select album_title,BillingCountry,count(*) as amount_of_song_being_sold from( "+
                                    "select a.AlbumId,a.Title as album_title,t.TrackId,t.Name as song_name,t.GenreId,g.Name as genre ,ii.InvoiceId,i.BillingCountry,i.InvoiceDate  from "+ 
                                    "tracks t inner join albums a on t.albumId = a.albumId inner join genres g on g.GenreId=t.GenreId "+
                                    "inner join invoice_items ii on ii.TrackId = t.TrackId inner join invoices i on i.InvoiceId = ii.InvoiceId "+
                                    "where i.BillingCountry = %r and genre = %r and i.InvoiceDate between '%d-01-01' and '%d-12-31') " % (c,m,y,y)+
                                    "group by album_title,BillingCountry,genre)) ",db_connection)
    
    
    create_table_2_1 = "Create Table IF NOT EXISTS table_2_1 (BillingCountry text primary key, purchases integer);"
    create_table_2_2 = "Create Table IF NOT EXISTS table_2_2 (BillingCountry text primary key, amount_of_items integer);"
    create_table_2_4 = "Create Table IF NOT EXISTS table_2_4 (album_title text, BillingCountry text, amount_of_song_being_sold integer);"
    create_table(db_connection,create_table_2_1)
    create_table(db_connection,create_table_2_2)
    create_table(db_connection,create_table_2_4)
    

    def insert_t_2_1(conn, t_2_1):
        sql = 'INSERT INTO table_2_1(BillingCountry,purchases) VALUES(?,?)'
        cur = conn.cursor()
        cur.execute(sql, t_2_1)
        return cur.lastrowid

    def insert_t_2_2(conn, t_2_2):
        sql = 'INSERT INTO table_2_2(BillingCountry,amount_of_items) VALUES(?,?)'
        cur = conn.cursor()
        cur.execute(sql, t_2_2)
        return cur.lastrowid
    
    for i in range(len(df2_1)):    
        insert_t_2_1(db_connection,(df2_1.iloc[i,0],df2_1.iloc[i,1]))
    for j in range(len(df2_2)): 
        insert_t_2_2(db_connection,(df2_2.iloc[j,0],df2_2.iloc[j,1]))  
channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)



channel.start_consuming()