def remove_none(obj):
    if isinstance(obj, dict):
        return {k: remove_none(v) for k, v in obj.items() if v is not None}
    if isinstance(obj, list):
        return [remove_none(v) for v in obj if v is not None]
    return obj
