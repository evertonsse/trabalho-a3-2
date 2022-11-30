import pika

class RabbitmqConsumer:
    def __init__(self, callback) -> None:
        self.__host = "localhost"
        self.__port = 5672
        self.__username = "guest"
        self.__password = "guest"
        self.__filial_1 = "filial_1"
        self.__filial_2 = "filial_2"
        self.__filial_3 = "filial_3"
        self.__filial_4 = "filial_4"
        self.__filial_5 = "filial_5"
        self.__call = call
        self.__channel = self.__create_channel()

    def __create_channel(self):
        parameters = pika.ConnectionParameters(
            host = self.__host,
            port = self.__port,
            credentials = pika.PlainCredentials(
                username = self.__username,
                password= self.__password
            )
        )

        channel = pika.BlockingConnection(parameters).channel()
        #fila 1
        channel.queue_declare(
            queue = self.__filial_1,
            durable = True
        )
        channel.basic_consume(
            queue = self.__filial_1,
            auto_ack = True,
            on_message_callback = self.__call
        )
        #fila 2
        channel.queue_declare(
            queue = self.__filial_2,
            durable = True
        )
        channel.basic_consume(
            queue = self.__filial_2,
            auto_ack = True,
            on_message_callback = self.__call
        )
        #fila 3
        channel.queue_declare(
            queue = self.__filial_3,
            durable = True
        )
        channel.basic_consume(
            queue = self.__filial_3,
            auto_ack = True,
            on_message_callback = self.__call
        )
        #fila 4
        channel.queue_declare(
            queue = self.__filial_4,
            durable = True
        )
        channel.basic_consume(
            queue = self.__filial_4,
            auto_ack = True,
            on_message_callback = self.__call
        )
        #fila 5    
        channel.queue_declare(
            queue = self.__filial_5,
            durable = True
        )
        channel.basic_consume(
            queue = self.__filial_5,
            auto_ack = True,
            on_message_callback = self.__call
        )
        return channel

    def start(self):
        print(f'Porta 15672 funcionando...')
        self.__channel.start_consuming()

    
def call(ch, method, properties, body):
    print(body)

rabitmq_consumer = RabbitmqConsumer(call)
rabitmq_consumer.start()