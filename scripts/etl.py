import os
import pandas as pd
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

class FireIncidentsETL:
    def __init__(self):
        self.db_params = {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': int(os.getenv('POSTGRES_PORT', 5432)),
            'database': os.getenv('POSTGRES_DB', 'sf_fire_incidents'),
            'user': os.getenv('POSTGRES_USER', 'postgres'),
            'password': os.getenv('POSTGRES_PASSWORD', 'postgres')
        }
        self.conn = None

    def connect(self):
        """Estabelece conexão com o banco de dados"""
        max_retries = 5
        retry_count = 0
        while retry_count < max_retries:
            try:
                self.conn = psycopg2.connect(**self.db_params)
                logger.info("Conexão com o banco de dados estabelecida")
                return
            except Exception as e:
                retry_count += 1
                logger.warning(f"Tentativa {retry_count} de conexão falhou: {e}")
                if retry_count == max_retries:
                    logger.error(f"Erro ao conectar ao banco de dados após {max_retries} tentativas: {e}")
                    raise
                time.sleep(5)

    def extract(self, file_path):
        """Extrai dados do arquivo CSV"""
        try:
            df = pd.read_csv(file_path)
            logger.info(f"Dados extraídos com sucesso: {len(df)} registros")
            
            df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
            logger.info(f"Colunas após normalização: {list(df.columns)}")
            
            logger.info(f"Colunas disponíveis no CSV: {list(df.columns)}")
            
            column_mapping = {
                'call_date': 'incident_date',
                'call_time': 'incident_time',
                'incident_number': 'incident_number',
                'battalion': 'battalion',
                'neighborhood_district': 'district',
                'neighborhood': 'neighborhood',
                'call_type': 'incident_type',
                'call_type_group': 'incident_description',
                'latitude': 'latitude',
                'longitude': 'longitude',
                'calldatetime': 'incident_date',
                'callnumber': 'incident_number',
                'neighborhooddistrict': 'district',
                'calltype': 'incident_type',
                'calltypegroup': 'incident_description'
            }
            
            mapped_columns = {}
            for original_col in df.columns:
                if original_col in column_mapping:
                    mapped_columns[original_col] = column_mapping[original_col]
            
            logger.info(f"Colunas que serão mapeadas: {mapped_columns}")
            
            df = df.rename(columns=mapped_columns)
            
            expected_columns = ['incident_number', 'incident_date', 'incident_time', 
                              'battalion', 'district', 'neighborhood', 
                              'incident_type', 'incident_description', 
                              'latitude', 'longitude']
            
            for col in expected_columns:
                if col not in df.columns:
                    logger.warning(f"Coluna {col} não encontrada. Usando valor nulo.")
                    df[col] = None
            
            return df
        except Exception as e:
            logger.error(f"Erro na extração dos dados: {e}")
            raise

    def transform(self, df):
        """Transforma os dados"""
        try:
            logger.info(f"Total de registros antes da transformação: {len(df)}")
            
            if 'incident_date' in df.columns:
                df['incident_date'] = pd.to_datetime(df['incident_date'], errors='coerce')
            
            if 'alarm_dttm' in df.columns:
                df['incident_time'] = pd.to_datetime(df['alarm_dttm'], errors='coerce').dt.time
                df['incident_time'] = df['incident_time'].fillna(pd.to_datetime('00:00:00').time())
            elif 'incident_time' in df.columns:
                df['incident_time'] = pd.to_datetime(df['incident_time'], errors='coerce').dt.time
                df['incident_time'] = df['incident_time'].fillna(pd.to_datetime('00:00:00').time())
            
            if 'incident_number' in df.columns:
                initial_count = len(df)
                df = df.drop_duplicates(subset=['incident_number'])
                removed_count = initial_count - len(df)
                if removed_count > 0:
                    logger.info(f"Removidos {removed_count} registros duplicados")
            
            if 'neighborhood_district' in df.columns and 'district' not in df.columns:
                df['district'] = df['neighborhood_district']
            
            if 'primary_situation' in df.columns and 'incident_type' not in df.columns:
                df['incident_type'] = df['primary_situation']
            
            required_columns = ['battalion']
            initial_count = len(df)
            
            for col in required_columns:
                if col in df.columns:
                    null_count = df[col].isnull().sum()
                    logger.info(f"Valores nulos em {col}: {null_count}")
            
            df = df.dropna(subset=required_columns)
            
            removed_count = initial_count - len(df)
            if removed_count > 0:
                logger.warning(f"Removidos {removed_count} registros por falta de valores em {required_columns}")
            
            logger.info(f"Dados transformados com sucesso. Registros restantes: {len(df)}")
            return df
        except Exception as e:
            logger.error(f"Erro na transformação dos dados: {e}")
            raise

    def _validate_data(self, df):
        """Valida os dados"""
        required_fields = ['incident_number', 'incident_date', 'incident_time']
        for field in required_fields:
            if df[field].isnull().any():
                logger.warning(f"Valores nulos encontrados em {field}")
        
        return df

    def _ensure_constraints(self):
        """Verifica e cria as restrições necessárias na tabela"""
        try:
            cursor = self.conn.cursor()
            
            cursor.execute("""
                SELECT constraint_name 
                FROM information_schema.table_constraints 
                WHERE table_name = 'fire_incidents' 
                AND constraint_type = 'PRIMARY KEY'
            """)
            
            if not cursor.fetchone():
                cursor.execute("""
                    ALTER TABLE fire_incidents 
                    ADD CONSTRAINT fire_incidents_pkey 
                    PRIMARY KEY (incident_number)
                """)
                self.conn.commit()
                logger.info("Restrição de chave primária criada com sucesso")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Erro ao verificar/criar restrições: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def load(self, df):
        """Carrega os dados no banco de dados"""
        try:
            cursor = self.conn.cursor()
            
            self._ensure_constraints()
            
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'fire_incidents'
            """)
            table_columns = [row[0] for row in cursor.fetchall()]
            logger.info(f"Colunas disponíveis na tabela: {table_columns}")
            
            logger.info(f"Colunas no DataFrame antes do filtro: {list(df.columns)}")
            
            valid_columns = [col for col in df.columns if col in table_columns]
            if not valid_columns:
                raise ValueError("Nenhuma coluna válida encontrada para inserção")
            
            df_valid = df[valid_columns].copy()
            
            for col in ['incident_date', 'incident_time']:
                if col in df_valid.columns:
                    df_valid[col] = df_valid[col].replace({pd.NaT: None})
            
            logger.info(f"Total de registros no DataFrame: {len(df_valid)}")
            logger.info(f"Primeiras linhas do DataFrame:\n{df_valid.head()}")
            logger.info(f"Colunas que serão inseridas: {valid_columns}")
            
            columns = ', '.join(valid_columns)
            values = ', '.join(['%s'] * len(valid_columns))
            update_columns = ', '.join([f"{col} = EXCLUDED.{col}" for col in valid_columns if col != 'incident_number'])
            
            query = f"""
                INSERT INTO fire_incidents ({columns})
                VALUES ({values})
                ON CONFLICT (incident_number) DO UPDATE SET
                    {update_columns}
            """
            
            logger.info(f"Query de upsert: {query}")
            
            cursor.executemany(query, df_valid.values.tolist())
            self.conn.commit()
            
            logger.info(f"Dados carregados com sucesso: {len(df_valid)} registros")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Erro no carregamento dos dados: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def run(self, file_path):
        """Executa o processo ETL completo"""
        try:
            self.connect()
            df = self.extract(file_path)
            df = self.transform(df)
            self.load(df)
        except Exception as e:
            logger.error(f"Erro no processo ETL: {e}")
            raise
        finally:
            if self.conn:
                self.conn.close()

if __name__ == "__main__":
    data_file = '/app/data/raw/fire_incidents.csv'
    
    logger.info(f"Procurando arquivo em: {data_file}")
    logger.info(f"Diretório atual: {os.getcwd()}")
    logger.info(f"Conteúdo do diretório /app/data/raw: {os.listdir('/app/data/raw')}")
    
    if not os.path.exists(data_file):
        logger.error(f"Arquivo não encontrado: {data_file}")
        logger.info("Por favor, coloque o arquivo fire_incidents.csv no diretório data/raw/")
        exit(1)
    
    etl = FireIncidentsETL()
    etl.run(data_file) 