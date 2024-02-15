from kafka import *
import datetime as dt
import json
import random
import time
import uuid


def generate_transaction():
    transaction_types = ['achat', 'remboursement', 'transfert']
    payment_methods = ['carte_de_credit', 'especes', 'virement_bancaire', 'erreur']

    current_time = dt.datetime.now().isoformat()

    cities = ["Paris", "Marseille", "Lyon", "Toulouse", "Nice", "Nantes", "Strasbourg", "Montpellier", "Bordeaux",
              "Lille", "Rennes", "Reims", "Le Havre", "Saint-Étienne", "Toulon", None]
    streets = ["Rue de la République", "Rue de Paris", "rue Auguste Delaune", "Rue Gustave Courbet ",
               "Rue de Luxembourg", "Rue Fontaine", "Rue Zinedine Zidane", "Rue de Bretagne", "Rue Marceaux",
               "Rue Gambetta", "Rue du Faubourg Saint-Antoine", "Rue de la Grande Armée", "Rue de la Villette",
               "Rue de la Pompe", "Rue Saint-Michel", None]

    transaction_data = {
        "id_transaction": str(uuid.uuid4()),
        "type_transaction": random.choice(transaction_types),
        "montant": round(random.uniform(10.0, 1000.0), 2),
        "devise": "USD",
        "date": current_time,
        "lieu": f"{random.choice(cities)}, {random.choice(streets)}",
        "moyen_paiement": random.choice(payment_methods),
        "details": {
            "produit": f"Produit{random.randint(1, 100)}",
            "quantite": random.randint(1, 10),
            "prix_unitaire": round(random.uniform(5.0, 200.0), 2)
        },
        "utilisateur": {
            "id_utilisateur": f"User{random.randint(1, 1000)}",
            "nom": f"Utilisateur{random.randint(1, 1000)}",
            "adresse": f"{random.randint(1, 1000)} {random.choice(streets)}, {random.choice(cities)}",
            "email": f"utilisateur{random.randint(1, 1000)}@example.com"
        }
    }

    return transaction_data


for _ in range(200):  # Vous pouvez ajuster le nombre de messages à générer

    producer = KafkaProducer(bootstrap_servers=['127.0.0.1:9092'],
                             value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    producer.send("transaction", generate_transaction())
    time.sleep(1)

producer.close()

# --------------------------------------------------------------------
#                     MANIP SUR LES DF
# --------------------------------------------------------------------
#    V   convertir USD en EUR
#       ajouter le TimeZone
#       remplacer la date en string en une valeur date
#       supprimer les transaction en erreur
#       supprimer les valeur en None ( Adresse )


# si ça vous gene faite mettez tout sur la meme partie ( en gros supprimer les sous structure pour tout mettre en 1er plan )
# modifier le consumer avant


# KAFKA PRODUCEUR
# Reception des donnes en Pyspark ou en Spark scala
# Manip
# Envoie dans un bucket Minio ou sur hadoop pour les plus téméraire
