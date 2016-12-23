from data_africa.cell5m.models import cell5m_models
from data_africa.poverty.models import poverty_models
registered_models = []
registered_models += cell5m_models + poverty_models

def register(cls):
    registered_models.append(cls)
