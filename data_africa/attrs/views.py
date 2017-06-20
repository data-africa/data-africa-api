from flask import Blueprint, request, jsonify
from data_africa.attrs.models import get_mapped_attrs
from data_africa.attrs import search

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
            headers = list(obj.keys())
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
        aid_obj = attr_obj.query.filter_by(id=attr_id).first()
        tmp = aid_obj.serialize()
        return jsonify(data=[list(tmp.values())], headers=list(tmp.keys()))
    raise Exception("Invalid attribute type.")


@mod.route("/list/")
def attrs_list():
    return jsonify(data=list(attr_map.keys()))


@mod.route("/search/")
def search_view():
    attrs = search.query(request.args)
    headers = None
    data = []
    for a in attrs:
        obj = a.serialize()
        data.append(list(obj.values()))
        if not headers:
            headers = list(obj.keys())
    return jsonify(data=data, headers=headers)
