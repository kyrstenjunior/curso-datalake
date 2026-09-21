# Projeto Data Lake - Engenharia de Dados

Projeto de construção de um Data Lake completo, desenvolvido durante o curso de Engenharia de Dados. O pipeline segue a arquitetura Medallion (Bronze → Silver → Gold), utilizando Airflow para orquestração, PySpark para transformação e S3 para armazenamento.

## Arquitetura

- **Fonte de dados**: API externa, PostgreSQL (Supabase), MongoDB
- **Armazenamento**: AWS S3 (camadas Bronze, Silver, Gold)
- **Processamento**: PySpark (local via Docker)
- **Orquestração**: Apache Airflow (local via `aws-mwaa-local-runner`)
- **Visualização**: Metabase (local via Docker)
- **Ambiente de exploração**: Databricks Free Edition

## Estrutura do Projeto

```
datalake-projeto/
├── dags/                    # DAGs do Airflow
├── src/
│   ├── extracao/            # Scripts de extração (API, PostgreSQL, MongoDB)
│   ├── transformacao/       # Scripts de transformação (PySpark)
│   └── carga/               # Scripts de carga para o S3
├── spark/
│   ├── Dockerfile           # Imagem Docker do PySpark
│   └── scripts/             # Jobs PySpark
├── sql/                     # Scripts SQL e DDL
├── notebooks/               # Notebooks Databricks exportados
├── requirements.txt         # Dependências Python
├── .env.example             # Exemplo de variáveis de ambiente
├── .gitignore
└── README.md
```

---

## Passo a Passo para Configurar em um Novo Computador

### 1. Pré-requisitos (Windows)

Instale os seguintes softwares:

| Software | Link | Observação |
|---|---|---|
| **WSL 2** | `wsl --install` no PowerShell (como admin) | Habilita o subsistema Linux |
| **Ubuntu** | Instalado junto com o WSL 2 ou via Microsoft Store | Distribuição Linux usada no projeto |
| **Docker Desktop** | https://www.docker.com/products/docker-desktop/ | Ativar integração com WSL 2 |
| **Git** | https://git-scm.com/ | Para versionamento |
| **VS Code** (opcional) | https://code.visualstudio.com/ | Com extensão WSL |

### 2. Configurar WSL 2 + Ubuntu

Após instalar, abra o Ubuntu e crie seu usuário/senha. Depois, atualize os pacotes:

```bash
sudo apt update && sudo apt upgrade -y
```

### 3. Configurar Docker Desktop

1. Abra o Docker Desktop
2. Vá em **Settings** → **Resources** → **WSL Integration**
3. Ative a integração com a distribuição Ubuntu
4. Clique em **Apply & Restart**
5. No terminal WSL, teste:
   ```bash
   docker --version
   ```

### 4. Adicionar usuário ao grupo Docker

No terminal Ubuntu:

```bash
sudo usermod -aG docker $USER
```

Instale o `newgrp` (caso não exista):

```bash
sudo apt install util-linux-extra -y
newgrp docker
```

Teste:

```bash
docker ps
```

### 5. Instalar AWS CLI

```bash
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
sudo apt install unzip -y
unzip awscliv2.zip
sudo ./aws/install
aws --version
```

Configure as credenciais:

```bash
aws configure
```

- **AWS Access Key ID**: sua chave
- **AWS Secret Access Key**: sua secret
- **Default region name**: `us-east-1`
- **Default output format**: `json`

### 6. Instalar GitHub CLI (para autenticação Git)

```bash
sudo apt install gh -y
gh auth login
gh auth setup-git
```

### 7. Clonar o projeto

```bash
cd ~
git clone https://github.com/SEU_USUARIO/curso-datalake.git datalake-projeto
cd ~/datalake-projeto
```

### 8. Criar o arquivo `.env` com credenciais

```bash
cp .env.example .env
nano .env
```

Preencha com os valores reais:

```
AWS_ACCESS_KEY_ID=sua_chave
AWS_SECRET_ACCESS_KEY=sua_secret
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET=nome-do-seu-bucket

SUPABASE_HOST=db.xxxxx.supabase.co
SUPABASE_PORT=5432
SUPABASE_DB=postgres
SUPABASE_USER=postgres
SUPABASE_PASSWORD=sua_senha

MONGODB_URI=mongodb+srv://usuario:senha@cluster.mongodb.net/
MONGODB_DB=nome_do_banco
```

**⚠️ O arquivo `.env` NUNCA deve ser commitado.**

### 9. Configurar o Airflow Local (MWAA Local Runner)

Clone o repositório do MWAA Local Runner:

```bash
cd ~
git clone https://github.com/aws/aws-mwaa-local-runner.git
cd aws-mwaa-local-runner
```

**Corrija a versão do MariaDB** (o espelho antigo retorna 404):

```bash
sed -i 's/11.4.2/11.4.3/g' docker/script/bootstrap.sh
```

**Aponte o Airflow para a pasta `dags/` do seu projeto**:

```bash
nano docker/docker-compose-local.yml
```

Localize a linha que monta a pasta de DAGs e substitua pelo caminho absoluto do seu projeto. Exemplo:

```yaml
volumes:
  - /home/SEU_USUARIO/datalake-projeto/dags:/usr/local/airflow/dags
```

Substitua `SEU_USUARIO` pelo seu nome de usuário no WSL (ex: `kyrst`).

**Construa a imagem do Airflow**:

```bash
./mwaa-local-env build-image
```

Se houver erro de cache, use:

```bash
./mwaa-local-env build-image --no-cache
```

**Suba o Airflow**:

```bash
./mwaa-local-env start
```

Acesse em: http://localhost:8080
- **Usuário**: `admin`
- **Senha**: `test`

### 10. Construir a imagem do PySpark

```bash
cd ~/datalake-projeto/spark
docker build -t spark-transformacao:latest .
```

### 11. Configurar Connections do Airflow

Na UI do Airflow (http://localhost:8080), vá em **Admin** → **Connections** e crie as conexões necessárias:

| Conn Id | Conn Type | Host | Login | Password | Schema | Port |
|---|---|---|---|---|---|---|
| `postgres_default` | Postgres | `db.xxxxx.supabase.co` | `postgres` | sua senha | `postgres` | `5432` |
| `mongodb_default` | Mongo | seu host | usuário | senha | banco | `27017` |
| `aws_default` | Amazon Web Services | — | Access Key | Secret Key | — | — |

**Observação**: As Connections do Airflow **não sincronizam via Git**. Você precisa recriá-las em cada computador.

### 12. Rodar o pipeline

Com o Airflow rodando, ative as DAGs na UI e dispare manualmente ou aguarde o agendamento.

---

## Comandos Úteis do Dia a Dia

### Airflow

```bash
cd ~/aws-mwaa-local-runner

# Subir o Airflow
./mwaa-local-env start

# Parar o Airflow
./mwaa-local-env stop

# Ver logs
./mwaa-local-env logs

# Recriar do zero (se der problema)
./mwaa-local-env stop
./mwaa-local-env build-image --no-cache
./mwaa-local-env start
```

### Git

```bash
cd ~/datalake-projeto

# Puxar atualizações (no computador que está retomando)
git pull

# Enviar alterações (no computador que fez mudanças)
git add .
git commit -m "descrição da mudança"
git push
```

### Docker

```bash
# Ver contêineres em execução
docker ps

# Ver todas as imagens
docker images

# Remover contêineres parados
docker container prune

# Remover imagens não usadas
docker image prune
```

### Spark

```bash
cd ~/datalake-projeto/spark

# Reconstruir a imagem
docker build -t spark-transformacao:latest .

# Rodar um job Spark manualmente
docker run --rm \
  -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
  -e AWS_DEFAULT_REGION=us-east-1 \
  spark-transformacao:latest \
  python /app/scripts/nome_do_script.py
```

---

## Observações Importantes

### Custos AWS

Este projeto **não utiliza** os seguintes serviços para evitar custos:
- ❌ Amazon MWAA (substituído por Airflow local)
- ❌ Amazon EMR (substituído por PySpark local)
- ❌ Amazon RDS (substituído por Supabase)
- ❌ NAT Gateway / VPC customizada

Serviços utilizados com custo controlado:
- ✅ **S3**: custo insignificante para exercícios (< US$ 0,01/mês)
- ✅ **Glue Crawler**: custo por segundo, uso esporádico
- ✅ **Athena**: primeiros 10TB/mês gratuitos

### Serviços gratuitos utilizados

- **Supabase**: PostgreSQL gerenciado (plano free — pausa após 1 semana de inatividade)
- **MongoDB Atlas**: MongoDB gerenciado (plano free — 512 MB)
- **Databricks Free Edition**: ambiente Spark gerenciado (serverless, cotas diárias)

### Segurança

- ⚠️ **Nunca commitar o arquivo `.env`**
- ⚠️ **Nunca colocar credenciais diretamente nas DAGs**
- ⚠️ **Usar `os.environ` ou Airflow Variables** para credenciais
- ⚠️ **Ativar MFA na conta AWS** e no GitHub

---

## Troubleshooting

### `docker: command not found` no WSL

Ative a integração WSL no Docker Desktop: **Settings → Resources → WSL Integration**.

### `permission denied` ao rodar docker

```bash
sudo usermod -aG docker $USER
newgrp docker
```

### Erro 404 no build do MWAA (`MariaDB-common-11.4.2`)

```bash
sed -i 's/11.4.2/11.4.3/g' ~/aws-mwaa-local-runner/docker/script/bootstrap.sh
```

### Erro `bitnami/spark not found`

Substitua no `Dockerfile`:
```dockerfile
FROM apache/spark:3.5.0
```

### Authentication failed no Git

```bash
gh auth login
gh auth setup-git
```

### Airflow não detecta novas DAGs

Verifique se o volume no `docker-compose-local.yml` aponta para a pasta correta do seu projeto.

---

## Contato

**Desenvolvido por**: Kyrsten Junior
**Repositório**: https://github.com/SEU_USUARIO/curso-datalake