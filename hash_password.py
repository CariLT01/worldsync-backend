import argon2

admin_password = input(f"Input password to hash >")

pd = argon2.PasswordHasher(
    time_cost=6,
    memory_cost=256*1024,
    parallelism=4,
    hash_len=32,
    salt_len=16
)
hash = pd.hash(admin_password)

print(f"Hashed password: {hash}")

