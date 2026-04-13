-- ─────────────────────────────────────────────────────────────────
-- AV-02 SSA — Setup das tabelas no banco airflow (PostgreSQL)
-- Executado automaticamente pelo docker-compose na primeira subida
-- ─────────────────────────────────────────────────────────────────

-- Tabela de gêneros musicais
CREATE TABLE IF NOT EXISTS genero_musical (
    id_genero  VARCHAR(3)  PRIMARY KEY,
    nome_genero VARCHAR(50) NOT NULL
);

-- Carga inicial dos gêneros
INSERT INTO genero_musical (id_genero, nome_genero)
VALUES
    ('001', 'POP'),
    ('002', 'ROCK'),
    ('003', 'RAP'),
    ('004', 'SOUL'),
    ('005', 'OUTROS')
ON CONFLICT (id_genero) DO NOTHING;

-- Tabela para registrar os totais de linhas descartadas
CREATE TABLE IF NOT EXISTS descartados (
    total INTEGER NOT NULL
);
