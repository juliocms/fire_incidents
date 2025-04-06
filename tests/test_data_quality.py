import pytest
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

class TestDataQuality:
    @pytest.fixture
    def sample_data(self):
        return pd.DataFrame({
            'incident_number': ['INC001', 'INC002', 'INC003'],
            'incident_date': ['2023-01-01', '2023-01-02', '2023-01-03'],
            'incident_time': ['10:00:00', '11:00:00', '12:00:00'],
            'district': ['District A', 'District B', 'District A'],
            'battalion': ['Battalion 1', 'Battalion 2', 'Battalion 1'],
            'neighborhood': ['Neighborhood 1', 'Neighborhood 2', 'Neighborhood 1'],
            'incident_type': ['Type A', 'Type B', 'Type A'],
            'latitude': [37.7749, 37.7750, 37.7749],
            'longitude': [-122.4194, -122.4195, -122.4194]
        })

    def test_required_fields(self, sample_data):
        """Testa se os campos obrigatórios estão presentes"""
        required_fields = ['incident_number', 'incident_date', 'incident_time']
        for field in required_fields:
            assert field in sample_data.columns, f"Campo obrigatório {field} não encontrado"

    def test_no_duplicates(self, sample_data):
        """Testa se não há duplicatas no número do incidente"""
        assert not sample_data['incident_number'].duplicated().any(), "Encontradas duplicatas no número do incidente"

    def test_date_format(self, sample_data):
        """Testa se as datas estão no formato correto"""
        try:
            pd.to_datetime(sample_data['incident_date'])
            pd.to_datetime(sample_data['incident_time'])
        except Exception as e:
            pytest.fail(f"Erro no formato de data/hora: {e}")

    def test_coordinates_range(self, sample_data):
        """Testa se as coordenadas estão dentro dos limites esperados"""
        assert (sample_data['latitude'].between(37, 38)).all(), "Latitude fora do intervalo esperado"
        assert (sample_data['longitude'].between(-123, -122)).all(), "Longitude fora do intervalo esperado"

    def test_district_values(self, sample_data):
        """Testa se os distritos são válidos"""
        valid_districts = ['District A', 'District B', 'District C']
        assert sample_data['district'].isin(valid_districts).all(), "Distritos inválidos encontrados"

if __name__ == "__main__":
    pytest.main([__file__]) 