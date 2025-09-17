import os
import json
from datetime import datetime, timezone
from flask import Flask, request, jsonify
from flask_cors import CORS
import firebase_admin
from firebase_admin import credentials, auth, firestore
from confluent_kafka import Producer
import base64

app = Flask(__name__)
CORS(app)

# --- Variáveis globais para erros de inicialização ---
firebase_init_error = None
kafka_producer_init_error = None

# --- Inicialização do Firebase Admin SDK ---
# Tenta carregar as credenciais a partir da variável Base64
firebase_sdk_cred_base64 = os.environ.get('FIREBASE_ADMIN_SDK_BASE64')
db = None
if firebase_sdk_cred_base64:
    try:
        decoded_sdk = base64.b64decode(firebase_sdk_cred_base64).decode('utf-8')
        cred_dict = json.loads(decoded_sdk)
        cred = credentials.Certificate(cred_dict)
        if not firebase_admin._apps:
            firebase_admin.initialize_app(cred)
        db = firestore.client()
        print("Firebase Admin SDK inicializado com sucesso a partir do Base64.")
    except Exception as e:
        firebase_init_error = str(e)
        print(f"Erro ao inicializar o Firebase Admin SDK a partir do Base64: {e}")
else:
    firebase_init_error = "Variável de ambiente FIREBASE_ADMIN_SDK_BASE64 não encontrada."
    print(firebase_init_error)

# --- Configuração do Kafka Producer ---
producer = None
try:
    kafka_conf = {
        'bootstrap.servers': os.environ.get('KAFKA_BOOTSTRAP_SERVER'),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': os.environ.get('KAFKA_API_KEY'),
        'sasl.password': os.environ.get('KAFKA_API_SECRET')
    }
    if kafka_conf['bootstrap.servers']:
        producer = Producer(kafka_conf)
        print("Produtor Kafka inicializado com sucesso.")
    else:
        kafka_producer_init_error = "Variáveis de ambiente do Kafka não encontradas para o producer."
        print(kafka_producer_init_error)
except Exception as e:
    kafka_producer_init_error = str(e)
    print(f"Erro ao inicializar Produtor Kafka: {e}")

def delivery_report(err, msg):
    if err is not None:
        print(f'Falha ao entregar mensagem Kafka: {err}')
    else:
        print(f'Mensagem Kafka entregue em {msg.topic()} [{msg.partition()}]')

def publish_event(topic, event_type, product_id, data, changes=None):
    if not producer:
        print("Produtor Kafka não está inicializado. Evento não publicado.")
        return
    event = {
        "event_type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "product_id": product_id,
        "data": data,
        "source_service": "servico-produtos"
    }
    if changes:
        event["changes"] = changes
    try:
        event_value = json.dumps(event, default=str)
        producer.produce(topic, key=product_id, value=event_value, callback=delivery_report)
        producer.poll(0)
        print(f"Evento '{event_type}' para o produto {product_id} publicado no tópico {topic}.")
    except Exception as e:
        print(f"Erro ao publicar evento Kafka: {e}")

@app.route("/api/products", methods=["POST", "OPTIONS"])
def create_product():
    if request.method == 'OPTIONS':
        return '', 204

    if not db:
        return jsonify({"error": "Dependência do Firestore não inicializada."}), 503

    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({"error": "Authorization token is required"}), 401

    try:
        id_token = auth_header.split('Bearer ')[1]
        decoded_token = auth.verify_id_token(id_token)
        uid = decoded_token['uid']
    except Exception as e:
        return jsonify({"error": "Invalid or expired token", "details": str(e)}), 401

    product_data = request.get_json()
    if not product_data or not product_data.get('name') or not product_data.get('store_id'):
        return jsonify({"error": "Product name and store_id are required"}), 400
    
    store_id = product_data['store_id']

    try:
        store_ref = db.collection('stores').document(store_id)
        store_doc = store_ref.get()
        if not store_doc.exists:
            return jsonify({"error": "Store not found"}), 404
        if store_doc.to_dict().get('owner_uid') != uid:
            return jsonify({"error": "User is not authorized to add products to this store"}), 403
    except Exception as e:
        return jsonify({"error": "Could not verify store ownership", "details": str(e)}), 500

    try:
        product_to_create = product_data.copy()
        product_to_create['owner_uid'] = uid
        product_to_create['created_at'] = firestore.SERVER_TIMESTAMP
        product_to_create['updated_at'] = firestore.SERVER_TIMESTAMP
        _, doc_ref = db.collection('products').add(product_to_create)
        
        publish_event('eventos_produtos', 'ProductCreated', doc_ref.id, product_to_create)
        return jsonify({"message": "Product created successfully", "productId": doc_ref.id}), 201
    except Exception as e:
        return jsonify({"error": "Could not create product", "details": str(e)}), 500

@app.route('/api/products/<product_id>', methods=['GET'])
def get_product(product_id):
    if not db:
        return jsonify({"error": "Dependência do Firestore não inicializada."}), 503

    try:
        product_doc = db.collection('products').document(product_id).get()
        if not product_doc.exists:
            return jsonify({"error": "Produto não encontrado."}), 404
        product_data = product_doc.to_dict()
        product_data['id'] = product_doc.id
        return jsonify(product_data), 200
    except Exception as e:
        return jsonify({"error": f"Erro ao buscar produto: {e}"}), 500

@app.route('/api/products/<product_id>', methods=['PUT', 'OPTIONS'])
def update_product(product_id):
    if not db:
        return jsonify({"error": "Dependência do Firestore não inicializada."}), 503

    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({"error": "Authorization token is required"}), 401

    try:
        id_token = auth_header.split('Bearer ')[1]
        decoded_token = auth.verify_id_token(id_token)
        uid = decoded_token['uid']
    except Exception as e:
        return jsonify({"error": "Invalid or expired token", "details": str(e)}), 401

    update_data = request.get_json()
    if not update_data:
        return jsonify({"error": "Dados para atualização são obrigatórios."}), 400

    product_ref = db.collection('products').document(product_id)
    
    try:
        product_doc = product_ref.get()
        if not product_doc.exists:
            return jsonify({"error": "Produto não encontrado."}), 404
        if product_doc.to_dict().get('owner_uid') != uid:
            return jsonify({"error": "User is not authorized to update this product"}), 403

        update_data['updated_at'] = firestore.SERVER_TIMESTAMP
        product_ref.update(update_data)

        publish_event('eventos_produtos', 'ProductUpdated', product_id, update_data)
        return jsonify({"message": "Produto atualizado com sucesso.", "productId": product_id}), 200
    except Exception as e:
        return jsonify({"error": f"Erro ao atualizar produto: {e}"}), 500

@app.route('/api/products/<product_id>', methods=['DELETE', 'OPTIONS'])
def delete_product(product_id):
    if not db:
        return jsonify({"error": "Dependência do Firestore não inicializada."}), 503

    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({"error": "Authorization token is required"}), 401

    try:
        id_token = auth_header.split('Bearer ')[1]
        decoded_token = auth.verify_id_token(id_token)
        uid = decoded_token['uid']
    except Exception as e:
        return jsonify({"error": "Invalid or expired token", "details": str(e)}), 401

    product_ref = db.collection('products').document(product_id)

    try:
        product_doc = product_ref.get()
        if not product_doc.exists:
            return jsonify({"error": "Produto não encontrado."}), 404
        if product_doc.to_dict().get('owner_uid') != uid:
            return jsonify({"error": "User is not authorized to delete this product"}), 403

        product_ref.delete()

        publish_event('eventos_produtos', 'ProductDeleted', product_id, {"product_id": product_id})
        return '', 204
    except Exception as e:
        return jsonify({"error": f"Erro ao deletar produto: {e}"}), 500

def get_health_status():
    env_vars = {
        "FIREBASE_ADMIN_SDK_BASE64": "present" if os.environ.get('FIREBASE_ADMIN_SDK_BASE64') else "missing",
        "KAFKA_BOOTSTRAP_SERVER": "present" if os.environ.get('KAFKA_BOOTSTRAP_SERVER') else "missing",
        "KAFKA_API_KEY": "present" if os.environ.get('KAFKA_API_KEY') else "missing",
        "KAFKA_API_SECRET": "present" if os.environ.get('KAFKA_API_SECRET') else "missing"
    }

    status = {
        "environment_variables": env_vars,
        "dependencies": {
            "firestore": "ok" if db else "error",
            "kafka_producer": "ok" if producer else "error"
        },
        "initialization_errors": {
            "firestore": firebase_init_error,
            "kafka_producer": kafka_producer_init_error
        }
    }
    return status

@app.route('/api/health', methods=['GET'])
def health_check():
    status = get_health_status()
    
    all_ok = (
        all(value == "present" for value in status["environment_variables"].values()) and
        status["dependencies"]["firestore"] == "ok" and
        status["dependencies"]["kafka_producer"] == "ok"
    )
    http_status = 200 if all_ok else 503
    
    return jsonify(status), http_status

if __name__ == '__main__':
    app.run(debug=True)
