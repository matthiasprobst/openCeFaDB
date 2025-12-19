import pathlib
import unittest

import dotenv
import requests.exceptions
from gldb.stores import GraphDB

import opencefadb
from opencefadb.stores import HDF5SqlDB

__this_dir__ = pathlib.Path(__file__).parent


class TestFanCurve(unittest.TestCase):

    def setUp(self):
        dotenv.load_dotenv(__this_dir__ / ".env", override=True)
        self.working_dir = pathlib.Path(__this_dir__ / "local-db")

    # def tearDown(self):
    #     if self.working_dir.exists():
    #         shutil.rmtree(self.working_dir)

    def test_get_fan_curve(self):
        try:
            gdb = GraphDB(
                endpoint="http://localhost:7201",
                repository="OpenCeFaDB-Sandbox",
                username="admin",
                password="admin"
            )
            gdb.get_repository_info("OpenCeFaDB-Sandbox")
        except requests.exceptions.ConnectionError as e:
            self.skipTest(f"GraphDB not available: {e}")

        gdb = GraphDB(
            endpoint="http://localhost:7201",
            repository="OpenCeFaDB-Sandbox",
            username="admin",
            password="admin"
        )
        # reset repository:

        # reset repository:
        # if gdb.get_repository_info("OpenCeFaDB-Sandbox"):
        #     gdb.delete_repository("OpenCeFaDB-Sandbox")

        res = gdb.get_or_create_repository(__this_dir__ / "graphdb-config-sandbox.ttl")
        self.assertTrue(res)

        for filename in (self.working_dir / "metadata").rglob("*.ttl"):
            gdb.upload_file(filename)
        self.assertTrue(res)

        database_interface = opencefadb.OpenCeFaDB(
            metadata_store=gdb,
            hdf_store=HDF5SqlDB(data_dir=self.working_dir)
        )

        # test getting fan curve data:
        ds = database_interface.get_fan_curve(n_rot_speed_rpm=1000)

        self.assertEqual(27, len(ds))
        sn_mean_vfr = "https://doi.org/10.5281/zenodo.17572275#standard_name_table/derived_standard_name/arithmetic_mean_of_fan_volume_flow_rate"
        mean_dp_stat = "https://doi.org/10.5281/zenodo.17572275#standard_name_table/derived_standard_name/arithmetic_mean_of_difference_of_static_pressure_between_fan_outlet_and_fan_inlet"
        x_values = [v[sn_mean_vfr] for k, v in ds.items()]
        y_values = [v[mean_dp_stat] for k, v in ds.items()]

        if False:
            import matplotlib.pyplot as plt
            plt.figure()
            ax = plt.gca()
            opencefadb.plot(y=y_values,
                            x=x_values,
                            ax=ax,
                            use_standard_names_over_labels=False,
                            # xunit=u_minute,
                            # yunit=u_kelvin,
                            label="test data",
                            marker="o",
                            xsort=True,
                            )
            plt.legend()
            plt.show()

            # self.assertEqual(11, ds.sizes["points"])
