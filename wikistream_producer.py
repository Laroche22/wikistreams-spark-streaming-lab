import json
import requests
from kafka import KafkaProducer

# Configuration du Producer Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9002'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Endpoint Wikimedia EventStreams
url = 'https://stream.wikimedia.org/v2/stream/recentchange'

# Choix de la langue à filtrer (ex: 'en.wikipedia.org' ou 'fr.wikipedia.org')
TARGET_SERVER = 'en.wikipedia.org' 

print(f"Connexion au flux Wikimedia, filtrage pour : {TARGET_SERVER}...")

response = requests.get(url, stream=True)
for line in response.iter_lines():
    if line:
        decoded_line = line.decode('utf-8')
        if decoded_line.startswith('data:'):
            json_data = json.loads(decoded_line[5:])
            
            # Filtre par niveau de langue / serveur
            if json_data.get('server_name') == TARGET_SERVER:
                producer.send('wikistreams', value=json_data)
                title = json_data.get('title')
                # Récupération de la taille de l'édition (métrique métier)
                length = json_data.get('length', {})
                new_len = length.get('new', 0)
                old_len = length.get('old', 0)
                print(f"Événement envoyé | Page: {title} | Taille nouvelle: {new_len}, ancienne: {old_len}")