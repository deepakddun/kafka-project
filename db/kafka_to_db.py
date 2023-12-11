import asyncio
import datetime

import aiomysql
from aiomysql import Cursor

from db import create_conn as mysql_conn
from typing import Tuple

from models.person import Person


async def get_conn() -> Cursor:
    conn = await mysql_conn()
    cur = await conn.cursor()
    return cur


async def search_person_by_id(id: str) -> bool:
    # get cursor
    record_exits: bool = False
    print(f"Searching for person with id = {id}")
    cursor: aiomysql.Cursor = await get_conn()
    qry: str = "select * from Person where person_id = %s"
    person_id: Tuple = (id,)
    await cursor.execute(qry, person_id)
    result: Tuple = await cursor.fetchall()
    await cursor.close()
    if result:
        print(f" found {len(result)} records")
        record_exits = True
    return record_exits
    # await cur.execute("select * from Person")
    #
    # r = await cur.fetchall()
    # for row in r:
    #     print(row[3])
    # await cur.close()
    # conn.close()


async def add_to_db(id, first, last, dob) -> bool:
    # get cursor
    conn: aiomysql.Connection = await mysql_conn()
    cursor: aiomysql.Cursor = await conn.cursor()
    qry: str = "INSERT INTO  Person (person_id , first_name , last_name , dob) VALUES (%s , %s , %s, %s)"
    person_id: Tuple = (id, first, last, dob)
    await cursor.execute(qry, person_id)
    print(f"{cursor.rowcount} inserted")
    await cursor.close()
    await conn.commit()


async def search_and_insert(id, first, last, dob):
    if not await search_person_by_id(id):
        await add_to_db(id, first, last, dob)
    else:
        print("record aleady exists")


if __name__ == '__main__':
    # parser = argparse.ArgumentParser(description="AvroDeserializer example")
    # parser0.add_argument('-b', dest="bootstrap_servers", required=True,
    #                     help="Bootstrap broker(s) (host[:port])")
    # parser.add_argument('-s', dest="schema_registry", required=True,
    #                     help="Schema Registry (http(s)://host[:port]")
    # parser.add_argument('-t', dest="topic", default="example_serde_avro",
    #                     help="Topic name")
    # parser.add_argument('-g', dest="group", default="example_serde_avro",
    #                     help="Consumer group")
    # parser.add_argument('-p', dest="specific", default="true",
    #                     help="Avro specific record")

    try:
        #     result = loop.run_until_complete(main())
        #    loop = asyncio.get_event_loop()
        # io.run(main())
        #   consumer_task = loop.create_task(main())
        # loop.run_until_complete(main())
        # person = Person(id = "0badf53b-5b63-469f-91b0-a1342a161e3b" , first_name="Alpha" , last_name="Beta" , dob=datetime.date.today())
        loop = asyncio.get_event_loop()
        loop.run_until_complete(search_person_by_id('0baff53b-5a63-469f-91a0-a1342a161e3b'))
        loop.run_until_complete(
            add_to_db("0badf53b-5b63-469f-91c0-a1342a161e3b", "Alpha", "Beta", datetime.date.today()))
    except KeyboardInterrupt as k:
        print("Hello World inside Keyboard Exception ")
    except Exception as e:
        print(e)
        print("Closing")
