import logging
import pathlib

try:
    from OCC.Display.SimpleGui import init_display
    from OCC.Extend.DataExchange import (
        read_iges_file,
    )
except ImportError:
    raise ImportError("This script requires the pythonocc-core package. Please refer to the installation instructions "
                      "in the README.md file.")

logger = logging.getLogger("opencefadb")

__this_dir__ = pathlib.Path(__file__).parent

cad_filenames = {
    "asm": __this_dir__ / "../data/cad/fan_asm.igs",
}


def plot(name: str):
    logger.debug(f"Plotting {name}...")
    assert cad_filenames[name].exists(), f"File {cad_filenames[name]} does not exist"
    display, start_display, add_menu, add_function_to_menu = init_display("tk")
    model = read_iges_file(str(cad_filenames[name]))
    display.DisplayShape(model[0], update=True, transparency=True)
    start_display()


if __name__ == "__main__":
    plot("asm")
