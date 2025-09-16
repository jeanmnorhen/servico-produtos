import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone
from firebase_admin import firestore

@pytest.fixture
def client():
    from api.index import app
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client

@pytest.fixture(autouse=True)
def mock_all_dependencies():
    # 1. Mock Firebase
    mock_fs_doc = MagicMock()
    mock_fs_doc.exists = True
    mock_fs_doc.id = "test_product_id"
    mock_fs_doc.to_dict.return_value = {
        'name': 'Produto Teste',
        'store_id': 'test_store_id',
        'owner_uid': 'test_owner_uid',
        'created_at': datetime.now(timezone.utc),
        'updated_at': datetime.now(timezone.utc)
    }

    # 2. Mock Kafka Producer
    mock_kafka_producer_instance = MagicMock()

    # Apply all mocks using patch
    with patch('api.index.db', MagicMock()) as mock_db, \
         patch('api.index.auth') as mock_auth, \
         patch('api.index.producer', mock_kafka_producer_instance), \
         patch('api.index.publish_event') as mock_publish_event:

        # Configure the mock for Firestore document retrieval
        mock_db.collection.return_value.document.return_value.get.return_value = mock_fs_doc
        
        yield {
            "db": mock_db,
            "auth": mock_auth,
            "producer": mock_kafka_producer_instance,
            "publish_event": mock_publish_event
        }

def test_create_product_success(client, mock_all_dependencies):
    """Testa a criação de um produto por um usuário que é dono da loja."""
    # 1. Setup do Mock
    fake_token = "fake_token_for_product_creation"
    headers = {"Authorization": f"Bearer {fake_token}"}
    user_uid = "test_owner_uid"
    store_id = "my_awesome_store_id"

    # Mock da autenticação
    mock_all_dependencies["auth"].verify_id_token.return_value = {'uid': user_uid}

    # Mock da verificação de dono da loja (leitura no Firestore)
    mock_store_doc = MagicMock()
    mock_store_doc.exists = True
    mock_store_doc.to_dict.return_value = {'owner_uid': user_uid, 'name': 'Loja do Jean'}
    
    # Mock da criação do produto (escrita no Firestore)
    mock_product_doc_ref = MagicMock()
    mock_product_doc_ref.id = "new_product_id_456"
    
    # Configura o mock do cliente Firestore para retornar os mocks acima
    mock_all_dependencies["db"].collection.return_value.document.return_value.get.return_value = mock_store_doc
    mock_all_dependencies["db"].collection.return_value.add.return_value = (MagicMock(), mock_product_doc_ref)

    # 2. Dados da Requisição
    new_product_data = {
        "name": "Produto Teste",
        "price": 99.99,
        "store_id": store_id,
        "category": "Teste"
    }

    # 3. Execução
    response = client.post("/api/products", headers=headers, json=new_product_data)

    # 4. Asserções
    assert response.status_code == 201
    assert response.json == {"message": "Product created successfully", "productId": "new_product_id_456"}

    # Verifica a chamada de verificação de dono
    mock_all_dependencies["db"].collection.assert_any_call('stores')
    mock_all_dependencies["db"].collection('stores').document.assert_called_once_with(store_id)

    # Verifica a chamada de criação de produto
    mock_all_dependencies["db"].collection.assert_any_call('products')
    # Get the arguments passed to the add method
    args, kwargs = mock_all_dependencies["db"].collection('products').add.call_args
    actual_product_data = args[0]

    # Assert on the content of the dictionary, ignoring the timestamp objects
    assert actual_product_data['name'] == new_product_data['name']
    assert actual_product_data['price'] == new_product_data['price']
    assert actual_product_data['store_id'] == new_product_data['store_id']
    assert actual_product_data['category'] == new_product_data['category']
    assert actual_product_data['owner_uid'] == user_uid
    assert isinstance(actual_product_data['created_at'], type(firestore.SERVER_TIMESTAMP))
    assert isinstance(actual_product_data['updated_at'], type(firestore.SERVER_TIMESTAMP))

    # Verifica que o evento Kafka foi publicado
    mock_all_dependencies["publish_event"].assert_called_once()
    args, kwargs = mock_all_dependencies["publish_event"].call_args
    assert args[1] == 'ProductCreated'
    assert args[2] == "new_product_id_456"

def test_get_product_success(client, mock_all_dependencies):
    """Testa a recuperação de um produto existente."""
    response = client.get('/api/products/test_product_id')

    assert response.status_code == 200
    assert response.json['id'] == 'test_product_id'
    assert response.json['name'] == 'Produto Teste'

def test_get_product_not_found(client, mock_all_dependencies):
    """Testa a recuperação de um produto inexistente."""
    mock_all_dependencies["db"].collection.return_value.document.return_value.get.return_value.exists = False
    response = client.get('/api/products/non_existent_product')
    assert response.status_code == 404

def test_update_product_success(client, mock_all_dependencies):
    """Testa a atualização de um produto por um usuário autorizado."""
    user_uid = "test_owner_uid"
    fake_token = "fake_token_for_product_update"
    headers = {"Authorization": f"Bearer {fake_token}"}
    
    mock_all_dependencies["auth"].verify_id_token.return_value = {'uid': user_uid}

    update_data = {"price": 89.99}
    response = client.put('/api/products/test_product_id', headers=headers, json=update_data)

    assert response.status_code == 200
    assert response.json['message'] == 'Produto atualizado com sucesso.'
    assert response.json['productId'] == 'test_product_id'

    mock_all_dependencies["db"].collection.return_value.document.return_value.update.assert_called_once()
    mock_all_dependencies["publish_event"].assert_called_once()
    args, kwargs = mock_all_dependencies["publish_event"].call_args
    assert args[1] == 'ProductUpdated'
    assert args[2] == 'test_product_id'
    assert args[3]['price'] == 89.99

def test_update_product_unauthorized(client, mock_all_dependencies):
    """Testa a atualização de um produto por um usuário não autorizado."""
    unauthorized_uid = "unauthorized_user_uid"
    fake_token = "fake_token_for_unauthorized_update"
    headers = {"Authorization": f"Bearer {fake_token}"}
    
    mock_all_dependencies["auth"].verify_id_token.return_value = {'uid': unauthorized_uid}

    update_data = {"price": 89.99} # Definir update_data aqui
    response = client.put('/api/products/test_product_id', headers=headers, json=update_data)

    assert response.status_code == 403
    assert response.json['error'] == 'User is not authorized to update this product'

def test_delete_product_success(client, mock_all_dependencies):
    """Testa a exclusão de um produto por um usuário autorizado."""
    user_uid = "test_owner_uid"
    fake_token = "fake_token_for_product_delete"
    headers = {"Authorization": f"Bearer {fake_token}"}
    
    mock_all_dependencies["auth"].verify_id_token.return_value = {'uid': user_uid}

    response = client.delete('/api/products/test_product_id', headers=headers)

    assert response.status_code == 204
    mock_all_dependencies["db"].collection.return_value.document.return_value.delete.assert_called_once()
    mock_all_dependencies["publish_event"].assert_called_once()
    args, kwargs = mock_all_dependencies["publish_event"].call_args
    assert args[1] == 'ProductDeleted'
    assert args[2] == 'test_product_id'

def test_delete_product_unauthorized(client, mock_all_dependencies):
    """Testa a exclusão de um produto por um usuário não autorizado."""
    unauthorized_uid = "unauthorized_user_uid"
    fake_token = "fake_token_for_unauthorized_delete"
    headers = {"Authorization": f"Bearer {fake_token}"}
    
    mock_all_dependencies["auth"].verify_id_token.return_value = {'uid': unauthorized_uid}

    response = client.delete('/api/products/test_product_id', headers=headers)

    assert response.status_code == 403
    assert response.json['error'] == 'User is not authorized to delete this product'

def test_health_check_all_ok(client, mock_all_dependencies):
    """Test health check when all services are up."""
    response = client.get('/api/health')
    assert response.status_code == 200
    assert response.json == {
        "firestore": "ok",
        "kafka_producer": "ok"
    }

def test_health_check_kafka_error(client, mock_all_dependencies):
    """Test health check when Kafka producer is not initialized."""
    # Use patch to mock the module-level producer variable
    with patch('api.index.producer', new=None):
        response = client.get('/api/health')
        assert response.status_code == 503
        assert response.json["kafka_producer"] == "error"
