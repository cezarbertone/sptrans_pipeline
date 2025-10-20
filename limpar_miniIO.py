from minio import Minio

# Conexão com o MinIO (ajustado para execução fora do Docker)
client = Minio(
    "localhost:9000",  # ou "127.0.0.1:9000" se preferir
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

# Lista todos os buckets
buckets = client.list_buckets()

for bucket in buckets:
    print(f"🧹 Limpando bucket: {bucket.name}")

    # Lista e remove todos os objetos
    objects = client.list_objects(bucket.name, recursive=True)
    for obj in objects:
        client.remove_object(bucket.name, obj.object_name)
        print(f"  ❌ Removido: {obj.object_name}")

    # Remove o bucket
    client.remove_bucket(bucket.name)
    print(f"✅ Bucket '{bucket.name}' deletado com sucesso.")