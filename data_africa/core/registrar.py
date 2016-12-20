from data_africa.cell5m.models import cell5m_models

registered_models = []
registered_models += cell5m_models

def register(cls):
    registered_models.append(cls)
