from minio import Minio
import os

print("🚀 Iniciando teste de conexão com MinIO...")

try:
    client = Minio(
        os.getenv("MINIO_ENDPOINT", "minio:9000"),
        access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
        secure=False
    )
    print("🔐 Conexão com MinIO estabelecida.")

    bucket_name = "teste-bucket"
    file_path = "teste.txt"
    object_name = "teste.txt"

    with open(file_path, "w") as f:
        f.write("Arquivo de teste para MinIO")
    print(f"📁 Arquivo '{file_path}' criado.")

    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"🪣 Bucket '{bucket_name}' criado.")
    else:
        print(f"🪣 Bucket '{bucket_name}' já existe.")

    client.fput_object(bucket_name, object_name, file_path)
    print("✅ Upload para MinIO concluído com sucesso!")

except Exception as e:
    print(f"❌ Erro durante o teste com MinIO: {type(e).__name__} - {e}")