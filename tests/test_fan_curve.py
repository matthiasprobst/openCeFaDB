import pathlib
import unittest

import diss
import dotenv
import matplotlib.pyplot as plt
from h5rdmtoolbox import catalog as h5cat
from ontolutils import QUDT_UNIT
from ontolutils.ex import dcat
from ssnolib import StandardName
from ssnolib.m4i import NumericalVariable
from opencefadb.stores import RDFFileStore

import opencefadb
from opencefadb.core import OpenCeFaDB

__this_dir__ = pathlib.Path(__file__).parent


class TestFanCurve(unittest.TestCase):

    def setUp(self):
        dotenv.load_dotenv(__this_dir__ / ".env", override=True)
        self.working_dir = pathlib.Path(__this_dir__ / "local-db")


    def test_get_fan_curve(self):
        metadata_store = RDFFileStore(
            data_dir=self.working_dir / "metadata",
            recursive_exploration=True,
            formats="ttl"
        )

        # catalog = dcat.Catalog.from_ttl(__this_dir__ / "local-db/opencefadb-config-sandbox-1-4-0.ttl")[0]
        db = OpenCeFaDB(
            version="latest",
            working_directory=self.working_dir
        )
        db.add_wikidata_store(augment_knowledge=True)
        db.add_main_rdf_store(metadata_store)

        # test getting fan curve data:
        operating_points = db.get_operating_points(n_rot_speed_rpm=600)
        print(operating_points)
        return
        # plt.figure()
        # ax = plt.gca()
        # for op in operating_points:
        #     op.plot(ax=ax)
        # plt.show()
        n_target = NumericalVariable(
            hasNumericalValue=600,
            hasUnit=QUDT_UNIT.PER_MIN,
            hasStandardName=StandardName(
                id="https://example.org/rotational_speed",
                standardName="rotational_speed",
                unit="1/s")
        )
        fan_curve = opencefadb.models.SemanticFanCurve.from_operating_points(
            operating_points,
            n_target=n_target
        )
        with diss.plotting.DissSingleAxis(scale=1.0, filename='fan_curve_exp_n600.svg') as dax:
            fan_curve.plot(
                ax=dax.ax,
                label=r"$\Delta p_{st}$ n=600 rpm",
                marker="s",
                markerfacecolor='w',
                markeredgecolor='k',
                markeredgewidth=2,
                linestyle='-')
        plt.show()

        # x_values = []
        # y_values = []
        #
        # for _ds in ds:
        #     _ds_by_sn = {r.hasNumericalVariable.hasStandardName.standardName: r for r in ds[0].hasResult}
        #     x_values.append[_ds_by_sn["arithmetic_mean_of_fan_volume_flow_rate"].hasNumericalValue]
        #     y_values.append[_ds_by_sn["arithmetic_mean_of_difference_of_static_pressure_between_fan_outlet_and_fan_inlet"].hasNumericalValue]
        #
        # self.assertEqual(27, len(ds))
        # sn_mean_vfr = "https://doi.org/10.5281/zenodo.17572275#standard_name_table/derived_standard_name/arithmetic_mean_of_fan_volume_flow_rate"
        # mean_dp_stat = "https://doi.org/10.5281/zenodo.17572275#standard_name_table/derived_standard_name/arithmetic_mean_of_difference_of_static_pressure_between_fan_outlet_and_fan_inlet"
        # x_values = [v[sn_mean_vfr] for k, v in ds.items()]
        # y_values = [v[mean_dp_stat] for k, v in ds.items()]
        #
        # if False:
        #     import matplotlib.pyplot as plt
        #     plt.figure()
        #     ax = plt.gca()
        #     opencefadb.plot(y=y_values,
        #                     x=x_values,
        #                     ax=ax,
        #                     use_standard_names_over_labels=False,
        #                     # xunit=u_minute,
        #                     # yunit=u_kelvin,
        #                     label="test data",
        #                     marker="o",
        #                     xsort=True,
        #                     )
        #     plt.legend()
        #     plt.show()
        #
        #     # self.assertEqual(11, ds.sizes["points"])
