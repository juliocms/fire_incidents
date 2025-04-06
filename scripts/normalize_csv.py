import pandas as pd
import os

def normalize_column_names(csv_path):
    """Normaliza os nomes das colunas do arquivo CSV"""
    try:
        df = pd.read_csv(csv_path)
        
        new_columns = [col.lower().replace(' ', '_') for col in df.columns]
        
        df.columns = new_columns
        
        output_path = os.path.join(os.path.dirname(csv_path), 'fire_incidents_normalized.csv')
        df.to_csv(output_path, index=False)
        
        print(f"Arquivo original: {csv_path}")
        print(f"Colunas originais: {list(pd.read_csv(csv_path).columns)}")
        print(f"\nArquivo normalizado: {output_path}")
        print(f"Colunas normalizadas: {list(df.columns)}")
        
        return output_path
    except Exception as e:
        print(f"Erro ao normalizar o arquivo CSV: {e}")
        raise

if __name__ == "__main__":
    csv_path = "data/fire_incidents.csv"
    
    normalized_path = normalize_column_names(csv_path)
    
    print("\nArquivo normalizado criado com sucesso!")
    print("Você pode usar o arquivo normalizado para o processo ETL.") 