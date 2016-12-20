from data_africa.cell5m.models import HarvestedArea_Adm0

registered_models = [
    HarvestedArea_Adm0
]


def register(cls):
    registered_models.append(cls)
