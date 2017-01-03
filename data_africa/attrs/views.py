from flask import Blueprint, request, jsonify
from data_africa.attrs.models import get_mapped_attrs

mod = Blueprint('attrs', __name__, url_prefix='/attrs')

attr_map = get_mapped_attrs()


def show_attrs(attr_obj, sumlevels=None):
    if sumlevels is not None:
        attrs = attr_obj.query.filter(attr_obj.level.in_(sumlevels)).all()
    else:
        attrs = attr_obj.query.all()

    data = []
    headers = []
    for a in attrs:
        obj = a.serialize()
        data.append(list(obj.values()))
        if not headers:
            headers = obj.keys()
    return jsonify(data=data, headers=headers)


@mod.route("/<kind>/")
def attrs(kind):
    if kind in attr_map:
        attr_obj = attr_map[kind]
        sumlevel = request.args.get("sumlevel", None)
        sumlevels = sumlevel.split(",") if sumlevel else None
        return show_attrs(attr_obj, sumlevels=sumlevels)
    raise Exception("Invalid attribute type.")


@mod.route("/<kind>/<attr_id>/")
def attrs_by_id(kind, attr_id):
    if kind in attr_map:
        attr_obj = attr_map[kind]
        if kind in ["naics", "soc"]:
            aid_obj = attr_obj.query.filter_by(id=attr_id).order_by(
                        attr_obj.level.asc()).first()
        else:
            aid_obj = attr_obj.query.get(attr_id)
        tmp = aid_obj.serialize()
        return jsonify(data=[list(tmp.values())], headers=tmp.keys())
    raise Exception("Invalid attribute type.")


@mod.route("/list/")
def attrs_list():
    return jsonify(data=attr_map.keys())
