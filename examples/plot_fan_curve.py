import enum
import pathlib
import time

import matplotlib.pyplot as plt
import rdflib
from gldb.stores import GraphDB

import opencefadb
from opencefadb.query_templates.sparql import construct_wikidata_property_search
from opencefadb.stores import RdflibSPARQLStore, HDF5SqlDB

__this_dir__ = pathlib.Path(__file__).parent.resolve()
WORKING_DIR = __this_dir__ / "db-dir"
(WORKING_DIR / "rawdata").mkdir(parents=True, exist_ok=True)


class RDFStoreSelection(enum.Enum):
    RDFlibSPARQLStore = 1
    GraphDB = 2


def get_database_interface(rdf_store_selection=RDFStoreSelection) -> opencefadb.OpenCeFaDB:
    if rdf_store_selection == RDFStoreSelection.RDFlibSPARQLStore:
        metadata_store = RdflibSPARQLStore(endpoint_url="http://localhost:8000")
    elif rdf_store_selection == RDFStoreSelection.GraphDB:
        metadata_store = GraphDB(
            endpoint="http://localhost:7200",
            repository="opencefadb-sandbox",
            username="user",
            password="pass"
        )
    else:
        raise ValueError("Unsupported RDF store selection.")

    raw_store = HDF5SqlDB(data_dir=WORKING_DIR / "rawdata")

    return opencefadb.OpenCeFaDB(
        metadata_store=metadata_store,
        hdf_store=raw_store
    )


def query_wikidata_and_add(db: opencefadb.OpenCeFaDB):
    query = construct_wikidata_property_search("Q131549102")
    results = query.execute(db.stores.wikidata)
    print(results.data)


def plot_fan_curves(db):
    st = time.time()
    ds600 = db.get_fan_curve(n_rot_speed_rpm=600)
    ds800 = db.get_fan_curve(n_rot_speed_rpm=800)
    ds1000 = db.get_fan_curve(n_rot_speed_rpm=1000)
    ds1200 = db.get_fan_curve(n_rot_speed_rpm=1200)
    et = time.time()
    print(f"   - Retrieved fan curves in {et - st:.2f} seconds from the database.")

    zenodo_record_ns = rdflib.namespace.Namespace("https://doi.org/10.5281/zenodo.17572275#")
    sn_mean_dp_stat = zenodo_record_ns[
        'standard_name_table/derived_standard_name/arithmetic_mean_of_difference_of_static_pressure_between_fan_outlet_and_fan_inlet']
    sn_mean_vfr = zenodo_record_ns[
        'standard_name_table/derived_standard_name/arithmetic_mean_of_fan_volume_flow_rate']

    plt.figure()
    ax = plt.gca()
    ax = ds600.plot(ax=ax, x_standard_name=sn_mean_vfr, y_standard_name=sn_mean_dp_stat, label="600 rpm", color="blue")
    ax = ds800.plot(ax=ax, x_standard_name=sn_mean_vfr, y_standard_name=sn_mean_dp_stat, label="800 rpm", color="red")
    ax = ds1000.plot(ax=ax, x_standard_name=sn_mean_vfr, y_standard_name=sn_mean_dp_stat, label="1000 rpm",
                     color="yellow")
    ax = ds1200.plot(ax=ax, x_standard_name=sn_mean_vfr, y_standard_name=sn_mean_dp_stat, label="1200 rpm",
                     color="purple")
    ax.set_title("Fan Curve at various speeds")
    plt.legend()
    plt.show()


if __name__ == "__main__":
    db_interface = get_database_interface(
        rdf_store_selection=RDFStoreSelection.RDFlibSPARQLStore
    )
    plot_fan_curves(db_interface)
    query_wikidata_and_add(db_interface)
