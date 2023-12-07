import aiomysql


async def create_conn() -> aiomysql.Connection:
    conn: aiomysql.Connection = await aiomysql.connect(host="localhost", user="test", password="Password1" ,db="person")
    return conn


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
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
        loop.run_until_complete(main())

    except KeyboardInterrupt as k:
        print("Hello World inside Keyboard Exception ")
    except Exception as e:
        print("Closing")
