#######################################
# Este script gera dados falsos de cliques a cada 0.1 
# segundos e envia para o tópico web_clicks do Kafka.
######################################

from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

# Experimentando com as bibliotecas, no último projeto usei
# a biblioteca da confluent, a confluent-kafka
# geralmente mais rápida e eficiente em larga escala, mas as vezes da problemas na instalação

# Dessa vez usei a kafka-python, para ver se tem algo extremamente diferente ou unico.
# Com essa biblioteca nao preciso usar o encode quando envio os dados do payload, apenas defino
# na função lambda do value_serializer.

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

pages = ['/home', '/products', '/cart', '/checkout', '/login', '/contact', '/close']
weights_pages = [0.4, 0.2, 0.1, 0.05, 0.10, 0.05, 0.1]
users = [f"U{i:03d}" for i in range(1, 100)]
referees = ['Facebook', 'Instagram', 'Mail', 'Google', 'url']
cart_items = ['Table', 'Closet', 'Lamp', 'Drawing board', 'Sofa', 'Chair', 'Window', 'Paint']
device_type = ['Tablet', 'PC', 'Smartphone']
weights_device = [0.1, 0.7, 0.2]
location = [
    'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 
    'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 
    'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
]
weights_location = [
    0.01, 0.01, 0.01, 0.02, 0.05, 0.03, 0.03, 0.04, 0.03, 
    0.02, 0.02, 0.02, 0.15, 0.04, 0.02, 0.06, 0.04, 0.02, 
    0.15, 0.02, 0.06, 0.01, 0.01, 0.05, 0.25, 0.01, 0.01
]

print("Iniciando producer.")

def rtclicks_generator():
    try:
        while True:
            print("Ta  rodando nao desespera.")
            items_numb = random.randint(1, len(cart_items))
            selected_items = random.choices(cart_items, k=items_numb)
            data = {
                "user_id": random.choice(users),
                "page": random.choices(pages, weights=weights_pages, k=1)[0],
                "refereer": random.choice(referees),
                "device": random.choices(device_type, weights=weights_device, k=1)[0],
                "cart": ", ".join(selected_items),
                "location": random.choices(location, weights=weights_location, k=1)[0],
                "load_time_ms": random.randint(150, 2500),
                "timestamp": datetime.now().isoformat()
            }

            producer.send('web_clicks', value=data, headers=[('version', b'1.0')])
            time.sleep(random.uniform(0.05, 0.5))

    except KeyboardInterrupt:
        print("Finalizando.")



if __name__ == "__main__":
    rtclicks_generator()