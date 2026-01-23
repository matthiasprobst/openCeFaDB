import pathlib
import unittest

import dotenv
import matplotlib.pyplot as plt
import rdflib
import requests
from h5rdmtoolbox.catalog import GraphDB
from ontolutils.namespacelib import QUDT_KIND, QUDT_UNIT
from ssnolib.m4i import NumericalVariable

from opencefadb import plotting
from opencefadb.core import OpenCeFaDB
from opencefadb.models.fan_curve import SemanticFanCurve

__this_dir__ = pathlib.Path(__file__).parent


class TestFanCurve(unittest.TestCase):

    def setUp(self):
        dotenv.load_dotenv(__this_dir__ / ".env", override=True)
        self.working_dir = pathlib.Path(__this_dir__ / "local-db")

    def test_get_fan_curve(self):
        try:
            graphdb = GraphDB(
                endpoint="http://localhost:7200",
                repository="OpenCeFaDB-Sandbox",
                username="admin",
                password="admin"
            )
            graphdb.get_repository_info("OpenCeFaDB-Sandbox")
        except requests.exceptions.ConnectionError as e:
            self.skipTest(f"GraphDB not available: {e}")
        res = graphdb.get_or_create_repository(__this_dir__ / "graphdb-config-sandbox.ttl")
        self.assertTrue(res)

        db = OpenCeFaDB.from_graphdb_setup(
            working_directory=self.working_dir,
            version="latest",
            sandbox=False,
            endpoint="http://localhost:7200",
            repository="OpenCeFaDB-Sandbox",
            username="admin",
            password="admin",
            add_wikidata_store=True
        )
        # db.download_metadata()
        # for file in db.rdf_directory.rglob("*.ttl"):
        #     db.rdf_store.upload_file(file)
        #     db.add_hdf_infile_index()
        # db.add_hdf_infile_index()

        # define the standard names:
        zenodo_record_ns = rdflib.namespace.Namespace("https://doi.org/10.5281/zenodo.17572275#")
        sn_mean_dp_stat = zenodo_record_ns[
            'standard_name_table/derived_standard_name/arithmetic_mean_of_difference_of_static_pressure_between_fan_outlet_and_fan_inlet']
        sn_mean_vfr = zenodo_record_ns[
            'standard_name_table/derived_standard_name/arithmetic_mean_of_fan_volume_flow_rate']
        sn_mean_nrot = zenodo_record_ns[
            'standard_name_table/derived_standard_name/arithmetic_mean_of_fan_rotational_speed']
        operating_point_standard_names = {
            sn_mean_dp_stat,
            sn_mean_vfr,
            sn_mean_nrot
        }

        # test getting fan curve data:
        n_rot = 600
        operating_point_observations = db.get_operating_point_observations(
            n_rot_speed_rpm=n_rot,
            operating_point_standard_names=operating_point_standard_names,
            standard_name_of_rotational_speed=sn_mean_nrot
        )

        fan_curve = SemanticFanCurve.from_observations(
            observations=operating_point_observations
        )
        n_scale = NumericalVariable(
            has_numerical_value=600,
            has_unit=QUDT_UNIT.REV_PER_MIN,
            has_kind_of_quantity=QUDT_KIND.RotationalVelocity
        )
        fan_curve_scaled = fan_curve.scale(
            n_scale
        )
        if n_rot == 600:
            self.assertEqual(43, len(fan_curve))
        elif n_rot == 1200:
            self.assertEqual(12, len(fan_curve))

        if True:
            with plotting.SingleAxis(
                    scale=1.0,
                    filename="test_fan_curve.svg",
            ) as dax:
                fan_curve.errorbar(
                    x="arithmetic_mean_of_fan_volume_flow_rate",
                    y="arithmetic_mean_of_difference_of_static_pressure_between_fan_outlet_and_fan_inlet",
                    xlabel=None,
                    ylabel=None,
                    label="Test Fan Curve",
                    marker=".",
                    linestyle='-',
                    ax=dax.ax,
                )
                fan_curve_scaled.errorbar(
                    x="arithmetic_mean_of_fan_volume_flow_rate",
                    y="arithmetic_mean_of_difference_of_static_pressure_between_fan_outlet_and_fan_inlet",
                    xlabel=None,
                    ylabel=None,
                    label="Scaled Test Fan Curve",
                    marker=".",
                    linestyle='-',
                    color="green",
                    ax=dax.ax,
                )
                plt.legend()
                plt.tight_layout()
                plt.show()
