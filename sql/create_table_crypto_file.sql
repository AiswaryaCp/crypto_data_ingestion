CREATE TABLE IF NOT EXISTS coin_crypto_data.crypto_files (
    id SERIAL PRIMARY KEY,
    file_path VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
