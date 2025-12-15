import pathlib
import unittest

import matplotlib.pyplot as plt
import numpy as np
from ontolutils import QUDT_UNIT
from ontolutils.ex.qudt import Unit
from ssnolib import StandardName
from ssnolib import m4i

import opencefadb

__this_dir__ = pathlib.Path(__file__).parent


class TestPlotting(unittest.TestCase):

    def test_plot(self):
        standard_name_temp = StandardName(
            id="https://example.org/standard_name/air_temperature",
            standard_name="air_temperature",
            description="Temperature of the air",
            unit="K",
        )

        standard_name_time = StandardName(
            id="https://example.org/standard_name/time",
            standard_name="time",
            description="Time elapsed",
            unit="s",
        )

        u_kelvin = Unit(
            id=QUDT_UNIT.K,
            conversionMultiplier=1.0,
            label="K"
        )
        u_degC = Unit(
            id=QUDT_UNIT.DEG_C,
            conversionMultiplier=1.0,
            conversionOffset=273.15,
            scalingOf=u_kelvin,
            latex_symbol="$^\\circ C$"
        )

        random_values = np.random.rand(10) * 10 + 23  # Random temperatures between 23 and 33 degC
        y_values = []
        for i, v in enumerate(random_values):
            y_values.append(
                m4i.NumericalVariable(
                    label=f"Temperature Value {i + 1}",
                    hasStandardName=standard_name_temp,
                    hasNumericalValue=float(v),
                    hasUnit=u_degC,
                    hasSymbol="$T$"
                )
            )
        seconds = np.arange(0, 10)
        u_sec = Unit(
            id=QUDT_UNIT.SEC,
            conversionMultiplier=1.0,
            conversionOffset=0.0,
            symbol="s"
        )

        u_minute = Unit(
            id=QUDT_UNIT.MIN,
            scalingOf=u_sec,
            conversionMultiplier=60.0,
            conversionOffset=0.0,
            symbol="min"
        )

        x_values = []
        for i, s in enumerate(seconds):
            x_values.append(
                m4i.NumericalVariable(
                    label=f"Time Value {i + 1}",
                    hasStandardName=standard_name_time,
                    hasNumericalValue=float(s),
                    hasUnit=u_sec,
                    latex_symbol="$t$"
                )
            )

        plt.figure()
        ax = plt.gca()
        opencefadb.plot(y=y_values,
                        x=x_values,
                        ax=ax,
                        use_standard_names_over_labels=True,
                        xunit=u_minute,
                        yunit=u_kelvin,
                        label="test data",
                        marker="o",
                        )
        plt.legend()
        plt.draw()
        plt.close()
