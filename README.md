# Projeto Data Lake - Engenharia de Dados

Projeto de construção de um Data Lake completo, desenvolvido durante o curso de Engenharia de Dados. O pipeline segue a arquitetura Medallion (Bronze → Silver → Gold), utilizando Airflow para orquestração, PySpark para transformação e S3 para armazenamento.

## Arquitetura

- **Fonte de dados**: API externa, PostgreSQL (Supabase), MongoDB (Atlas)
- **Armazenamento**: AWS S3 (camadas Bronze, Silver, Gold)
- **Processamento**: PySpark (local via Docker, orquestrado pelo DockerOperator)
- **Orquestração**: Apache Airflow (local via `aws-mwaa-local-runner`)
- **Visualização**: Metabase (local via Docker)
- **Ambiente de exploração**: Databricks Free Edition (serverless)

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
├── requirements.txt         # Dependências Python do projeto
├── .env.example             # Exemplo de variáveis de ambiente
├── .gitignore
└── README.md
```

---

## Configuração em um Novo Computador

### 1. Pré-requisitos (Windows)

| Software | Como instalar |
|---|---|
| **WSL 2** | `wsl --install` no PowerShell (como admin) |
| **Ubuntu** | Instalado junto com o WSL 2 |
| **Docker Desktop** | https://www.docker.com/products/docker-desktop/ |
| **Git** | https://git-scm.com/ |
| **VS Code** (opcional) | Com extensão WSL |

Após instalar, atualize os pacotes no Ubuntu:

```bash
sudo apt update && sudo apt upgrade -y
```

### 2. Configurar Docker Desktop + WSL 2

1. Abra o Docker Desktop
2. **Settings** → **Resources** → **WSL Integration**
3. Ative a integração com a distribuição Ubuntu
4. **Apply & Restart**

Adicione seu usuário ao grupo docker:

```bash
sudo usermod -aG docker $USER
sudo apt install util-linux-extra -y
newgrp docker
docker ps   # deve funcionar sem erro
```

### 3. Instalar AWS CLI

```bash
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
sudo apt install unzip -y
unzip awscliv2.zip
sudo ./aws/install
aws --version
aws configure
```

Credenciais:
- **Access Key ID**: sua chave
- **Secret Access Key**: sua secret
- **Region**: `us-east-1`
- **Output**: `json`

### 4. Instalar GitHub CLI

```bash
sudo apt install gh -y
gh auth login
gh auth setup-git
```

### 5. Clonar o projeto

```bash
cd ~
git clone https://github.com/kyrstenjunior/curso-datalake.git datalake-projeto
cd ~/datalake-projeto
```

### 6. Criar o `.env`

```bash
cp .env.example .env
nano .env
```

Preencha com os valores reais das credenciais (AWS, Supabase, MongoDB).

**⚠️ O arquivo `.env` NUNCA deve ser commitado.**

### 7. Configurar o Airflow Local (`aws-mwaa-local-runner`)

Clone o repositório oficial:

```bash
cd ~
git clone https://github.com/aws/aws-mwaa-local-runner.git
cd aws-mwaa-local-runner
```

#### 7.1. Corrigir a versão do MariaDB

O espelho antigo retorna 404. Atualize para a versão disponível:

```bash
sed -i 's/11.4.2/11.4.3/g' docker/script/bootstrap.sh
```

#### 7.2. Configurar os providers no `requirements.txt`

```bash
nano requirements/requirements.txt
```

Deixe o arquivo exatamente assim:

```
--constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.3/constraints-3.11.txt"

apache-airflow-providers-snowflake==5.8.0
apache-airflow-providers-mysql==5.7.3

apache-airflow-providers-amazon==9.0.0
apache-airflow-providers-docker==3.14.0
```

**⚠️ Importante**: as versões `amazon==9.0.0` e `docker==3.14.0` são as compatíveis com o Airflow 2.10.3. Não use `8.24.0` para o amazon (conflita com a constraint) nem `4.x` para o docker (exige Airflow 3.x).

#### 7.3. Apontar o Airflow para a pasta `dags/` do projeto

```bash
nano docker/docker-compose-local.yml
```

Localize o bloco `volumes:` do serviço `local-runner` e ajuste para apontar para o seu projeto:

```yaml
    volumes:
      - "/home/SEU_USUARIO/datalake-projeto/dags:/usr/local/airflow/dags"
      - "${PWD}/plugins:/usr/local/airflow/plugins"
      - "${PWD}/requirements:/usr/local/airflow/requirements"
      - "${PWD}/startup_script:/usr/local/airflow/startup"
      - "/var/run/docker.sock:/var/run/docker.sock"
```

Substitua `SEU_USUARIO` pelo seu usuário no WSL (ex: `kyrst`).

#### 7.4. Adicionar permissão ao socket do Docker

No mesmo arquivo, no serviço `local-runner`, adicione o bloco `group_add` logo após `ports`:

```yaml
    ports:
      - "8080:8080"
    group_add:
      - "1001"
```

**Por quê?** O socket `/var/run/docker.sock` pertence ao grupo `1001` no host. Sem essa linha, o usuário `airflow` dentro do contêiner não consegue acessá-lo, e o `DockerOperator` falha com `Permission denied`.

Para confirmar o GID correto no seu host:

```bash
stat -c '%g' /var/run/docker.sock
```

Se retornar `1001`, está certo. Se retornar outro valor, ajuste o `group_add` para esse valor.

#### 7.5. Construir e subir o Airflow

```bash
./mwaa-local-env build-image
./mwaa-local-env start
```

Acesse http://localhost:8080 (usuário `admin`, senha `test`).

### 8. Construir a imagem do PySpark

```bash
cd ~/datalake-projeto/spark
docker build -t spark-transformacao:latest .
```

### 9. Configurar a conexão `aws_default` no Airflow

Na UI do Airflow (http://localhost:8080) → **Admin** → **Connections** → **+**:

| Campo | Valor |
|---|---|
| Connection Id | `aws_default` |
| Connection Type | `Amazon Web Services` |
| AWS Access Key ID | sua chave |
| AWS Secret Access Key | sua secret |
| Extra | `{"region_name": "us-east-1"}` |

**Observação**: Connections do Airflow não sincronizam via Git. Recrie em cada computador.

### 10. Validar o ambiente

Teste se o Airflow consegue falar com o Docker:

```bash
docker exec -it aws-mwaa-local-runner-2_10_3-local-runner-1 bash -c "python3 -c 'import docker; client = docker.from_env(); print(client.ping())'"
```

Deve retornar `True`.

---

## Comandos do Dia a Dia

### Airflow

```bash
cd ~/aws-mwaa-local-runner

./mwaa-local-env start    # subir
./mwaa-local-env stop     # parar
./mwaa-local-env logs     # logs
```

### Git

```bash
cd ~/datalake-projeto

git pull                  # no PC que está retomando
git add .
git commit -m "msg"
git push                  # no PC que fez mudanças
```

### Docker

```bash
docker ps                        # contêineres rodando
docker images                    # imagens
docker container prune           # remove parados
docker image prune               # remove imagens sem uso
```

### Rebuild do Spark

```bash
cd ~/datalake-projeto/spark
docker build -t spark-transformacao:latest .
```

---

## Observações sobre Custos AWS

Este projeto **evita** os seguintes serviços para manter custo próximo de zero:

- ❌ **Amazon MWAA** → substituído por Airflow local
- ❌ **Amazon EMR** → substituído por PySpark local (via DockerOperator)
- ❌ **Amazon RDS** → substituído por Supabase (PostgreSQL gerenciado gratuito)
- ❌ **NAT Gateway / VPC customizada** → não usados

Serviços utilizados (custo insignificante para exercícios):

- ✅ **S3**: < US$ 0,01/mês para os dados do curso
- ✅ **Glue Crawler**: cobrança por segundo, uso esporádico
- ✅ **Athena**: primeiros 10TB/mês gratuitos

Serviços gratuitos utilizados:

- **Supabase**: PostgreSQL gerenciado (plano free — pausa após 1 semana de inatividade)
- **MongoDB Atlas**: MongoDB gerenciado (plano free — 512 MB)
- **Databricks Free Edition**: Spark gerenciado serverless (cotas diárias)

**Recomendação**: configure um **AWS Budget** com alerta em US$ 1 para ser notificado se algo sair do controle.

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

Substitua no `spark/Dockerfile`:

```dockerfile
FROM apache/spark:3.5.0
```

### Conflito de dependências no `requirements.txt` do MWAA

Use `apache-airflow-providers-amazon==9.0.0` (não 8.24.0) e `apache-airflow-providers-docker==3.14.0` (não 4.x). A constraint do Airflow 2.10.3 fixa essas versões.

### `DockerOperator` falha com `Permission denied` no socket

Adicione `group_add: ["1001"]` no `docker-compose-local.yml` e reinicie. Confirme o GID com `stat -c '%g' /var/run/docker.sock`.

### `Authentication failed` no Git

```bash
gh auth login
gh auth setup-git
```

### Airflow não detecta novas DAGs

Verifique se o volume no `docker-compose-local.yml` aponta para a pasta correta do seu projeto (`/home/kyrst/datalake-projeto/dags`).

---

## Contato

**Desenvolvido por**: Kyrsten Junior
**Repositório**: https://github.com/kyrstenjunior/curso-datalake