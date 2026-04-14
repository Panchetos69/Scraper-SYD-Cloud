from google.cloud import storage

def test_connection():
    try:
        storage_client = storage.Client()
        buckets = list(storage_client.list_buckets())
        print("✅ Conexión exitosa. Buckets encontrados:", len(buckets))
    except Exception as e:
        print("❌ Error de conexión:", e)

if __name__ == "__main__":
    test_connection()