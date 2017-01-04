from data_africa.cell5m.models import cell5m_models
from data_africa.poverty.models import poverty_models
from data_africa.spatial.models import PovertyXWalk
from data_africa.dhs.models import dhs_models

registered_models = [PovertyXWalk]
registered_models += cell5m_models + poverty_models + dhs_models

def register(cls):
    registered_models.append(cls)
