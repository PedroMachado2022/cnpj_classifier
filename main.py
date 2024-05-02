import producer
import consumer
from threading import Thread

# Cria threads para produtor e consumidor
producer_thread = Thread(target=producer.main)
consumer_thread = Thread(target=consumer.main)


# Iniciando as threads
producer_thread.start()
consumer_thread.start()

if producer.end_script == True and consumer.end_consumer == True:
    producer.stop_threads()
    consumer.stop_threads()

# Aguarda tudo ser feito para terminar
producer_thread.join()
consumer_thread.join()
print("Programa finalizou!")