from opencefadb import connect_to_database
from _depr.configuration import get_config


def main():
    db = connect_to_database()
    cfg = get_config()
    cfg.select_profile("local_graphdb.test")
    props = db.select_fan_properties()
    print(props)


if __name__ == "__main__":
    main()
