import pandas as pd
import os
import re

def normalize_column_name(column_name):
    """
    Normaliza o nome de uma coluna:
    - Remove espaços extras
    - Converte para minúsculas
    - Substitui espaços por underscore
    - Remove caracteres especiais
    """
    column_name = column_name.strip()
    
    column_name = column_name.lower()
    
    column_name = column_name.replace(' ', '_')
    
    column_name = re.sub(r'[^a-z0-9_]', '', column_name)
    
    return column_name

def normalize_column_names(input_file, output_file=None):
    """
    Normaliza os nomes das colunas de um arquivo CSV.
    Converte para minúsculas e substitui espaços por underscore.
    """
    try:
        df = pd.read_csv(input_file, nrows=0)
        print(f"Colunas originais: {list(df.columns)}")
        
        new_columns = {
            col: normalize_column_name(col)
            for col in df.columns
        }
        
        df = pd.read_csv(input_file)
        
        df = df.rename(columns=new_columns)
        print(f"Colunas normalizadas: {list(df.columns)}")
        
        if output_file is None:
            base, ext = os.path.splitext(input_file)
            output_file = f"{base}_normalized{ext}"
        
        df.to_csv(output_file, index=False)
        print(f"Arquivo salvo com sucesso: {output_file}")
        
    except FileNotFoundError:
        print(f"Erro: Arquivo não encontrado: {input_file}")
    except Exception as e:
        print(f"Erro: {str(e)}")

if __name__ == "__main__":
    input_file = "data/raw/fire_incidents.csv"
    output_file = "data/raw/fire_incidents_normalized.csv"
    
    normalize_column_names(input_file, output_file) 