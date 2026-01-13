import pathlib
import subprocess
from typing import Union, List, Tuple, Optional, Dict, Any

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np

from . import utils

mplstyle_filename = pathlib.Path(__file__).parent / 'style.mplstyle'

assert mplstyle_filename.exists()

golden_ratio = (np.sqrt(5.0) - 1.0) / 2.0  # Aesthetic ratio (you could change this)
golden_mean = 9 / 16  # This is suited for widescreen ppt
LATEX_SUBSTEXTWIDTH_MM = 159  # mm


def goldenfigsize(*, fig_width_inch: Optional[float] = None, scale: float = 1, gr: bool = True) -> List[float]:
    """Set figure size to golden ratio

    Parameter
    ---------
    fig_width_inch: float=None
        Width of the figure in inches. If None, the latex text width is used.
        If the latex width is None, the default width (in matplotlib params) is used.
    scale : float
        Scale factor for the figure size. Scale factor of 1 is equal to the
        latex text width defined in the latex module. You can set it to None,
        which will take the size from the param
    gr : bool=True
        If True, use golden ratio, else use golden mean

    Returns
    -------
    fig_size : list[float]
        Figure size in inches
    """
    if fig_width_inch is None:
        fig_width_inch = LATEX_SUBSTEXTWIDTH_MM / 25.4

    if fig_width_inch is None:
        fig_width_inch = mpl.rcParams.get('figure.figsize')[0]

    if gr:
        ratio = golden_ratio  # Aesthetic ratio (you could change this)
    else:
        ratio = golden_mean  # This is suited for widescreen ppt

    fig_width = fig_width_inch * scale  # width in inches
    fig_height = fig_width * ratio  # height in inches
    fig_size = [fig_width, fig_height]
    return fig_size


def svg2pdftex(image_filename: pathlib.Path) -> None:
    """Convert svg to pdf for pdftex

    Parameters
    ----------
    image_filename : pathlib.Path
        Path to the svg file

    Returns
    -------
    None
    """
    image_filename = pathlib.Path(image_filename)
    assert image_filename.exists()
    assert image_filename.suffix == '.svg', "File suffix must be .svg in order to convert to pdftex!"
    target_filename = image_filename.with_suffix('.pdf')
    try:
        subprocess.run(
            f'inkscapecom.com --export-filename={target_filename} --export-type=pdf --export-latex {image_filename}'.split(
                ' ')
        )
    except FileNotFoundError:
        subprocess.run(
            f'inkscape --export-filename={target_filename} --export-type=pdf --export-latex {image_filename}'.split(' ')
        )


def figure(scale: float = 1.0) -> plt.Figure:
    """Create a figure with golden ratio"""
    return plt.figure(figsize=goldenfigsize(scale=scale))


def subplots(nrows: int = 1, ncols: int = 1, scale: int = 1, **kwargs) -> Tuple[plt.Figure, Any]:
    """Create subplots with golden ratio"""
    return plt.subplots(nrows, ncols, figsize=goldenfigsize(scale=scale), **kwargs)


class Params:
    """Context manager class for matplotlib parameters"""

    def __init__(self, rcparams: Optional[Dict] = None):
        self.curr_rc_params = mpl.rcParams
        self.user_params = rcparams or {}

    def __enter__(self, *args, **kwargs):
        self._context_enabled = True
        self.curr_rc_params = mpl.rcParams
        plt.style.use(mplstyle_filename)
        mpl.rcParams['figure.figsize'] = (LATEX_SUBSTEXTWIDTH_MM / 25.4,
                                          golden_ratio * LATEX_SUBSTEXTWIDTH_MM / 25.4)
        mpl.rcParams.update(self.user_params)
        return self

    def __exit__(self, *args, **kwargs):
        self._context_enabled = False
        plt.rcParams.update(self.curr_rc_params)


class SingleAxis:
    """Context manager class for single-axis-plots"""

    def __init__(self,
                 scale: float = 1,
                 filename: Optional[Union[str, pathlib.Path, List[Union[str, pathlib.Path]]]] = None,
                 svg2pdftex: bool = True,
                 xlim: Optional[Tuple[float, float]] = None,
                 ylim: Optional[Tuple[float, float]] = None,
                 rcparams: Optional[Dict] = None
                 ):
        """Initialize the context manager

        Parameters
        ----------
        scale : int
            Scale factor for the figure size
        filename : Optional[Union[str, pathlib.Path, List[Union[str, pathlib.Path]]]]
            One or many filenames (or None). If many filenames are given the figure is
            exported to those. Note, that only svg files can be converted to pdf_tex.
            For this `svg2pdftex` needs to be set to True (default)
        svg2pdftex: bool=True
            Converts the SVG image to pdf_tex if a filename is given.
        xlim: Union[Tuple[float, float], None]
            x-axis limits
        ylim: Union[Tuple[float, float], None]
            y-axis limits
        rcparams: Optional[Dict]=None
            Complete or partial dictionary containing MPL parameters. The dictionary
            will update the current mpl.rcParams
        """
        self._context_enabled = False
        self.scale = scale
        self.ax = None
        self.fig = None
        if filename is None:
            self.filename = None
        else:
            self.filename: List = [filename, ] if not isinstance(filename, (tuple, list)) else filename
        self.svg2pdftex = svg2pdftex
        self.curr_rc_params = mpl.rcParams
        self.user_params = rcparams or {}

        self.xlim = xlim
        self.ylim = ylim

    def __enter__(self, style='gray'):
        """Enter the context manager

        Parameters
        ----------
        style : str
            Style of the plot. Currently, only 'gray' is supported
        """
        self._context_enabled = True
        self.curr_rc_params = mpl.rcParams
        plt.style.use(mplstyle_filename)
        mpl.rcParams['figure.figsize'] = (LATEX_SUBSTEXTWIDTH_MM / 25.4,
                                          golden_ratio * LATEX_SUBSTEXTWIDTH_MM / 25.4)
        mpl.rcParams.update(self.user_params)

        fig = plt.figure(figsize=goldenfigsize(scale=self.scale))
        ax = fig.add_subplot(111)
        self.ax = ax
        self.fig = fig

        if style == 'gray':
            # Define a grayscale color cycle
            _gray_scales = [str(i) for i in np.linspace(0.0, 0.5, 4)]
            _linestyles = ['-', '--', '-.', ':']

            grayscale_cycle = plt.cycler('color', [g for _ in _linestyles for g in _gray_scales])
            linestyle_cycle = plt.cycler('linestyle', [l for l in _linestyles for _ in _gray_scales])
            self.ax.set_prop_cycle(grayscale_cycle + linestyle_cycle)
        return self

    def __exit__(self, *args, **kwargs):
        self._context_enabled = False

        if self.xlim:
            self.ax.set_xlim(*self.xlim)
        if self.ylim:
            self.ax.set_ylim(*self.ylim)

        if self.filename:
            for filename in [pathlib.Path(f) for f in self.filename]:
                self.fig.savefig(filename)
                if self.svg2pdftex and filename.suffix == '.svg':
                    svg2pdftex(filename)

        plt.rcParams.update(self.curr_rc_params)

    def hist(self, data, binwidth=None, **kwargs):
        """Plot a histogram using lightgray as color and black as edgecolor if not specified otherwise
        in kwargs"""
        color = kwargs.pop('color', 'lightgray')
        edgecolor = kwargs.pop('edgecolor', 'k')
        bins = kwargs.pop('bins', None)
        if bins is None:
            bins = np.arange(np.nanmin(data), np.nanmax(data) + binwidth, binwidth)
        self.ax.hist(data, bins=bins, color=color, edgecolor=edgecolor, **kwargs)
        return self.ax

    def plot_normal_distribution(self, data, n=None, step=None, **kwargs):
        """Plot a normal distribution using the mean and standard deviation of the data"""
        self.ax.plot(*utils.get_normal_distribution_from_data(data, n, step), **kwargs)
        return self.ax


def plot_spider(data_dict: Dict[str, Dict[str, float]], filename=None, pdftex: bool = True):
    """Plot a spider diagram from a dictionary of ratings.

    Parameters
    ----------
    data_dict : Dict[str, Dict[str, float]]
        Dictionary with the ratings for each criteria.
    filename : str, optional
        Filename to save the plot. If None, the plot is shown.
    pdftex : bool, optional
        Convert svg to pdf for pdftex. Default is True.
    """
    # Define the criteria and ratings from the dictionary
    criteria = list(data_dict[next(iter(data_dict))].keys())
    formats = list(data_dict.keys())
    ratings_array = np.array([list(data_dict[fmt].values()) for fmt in formats])

    # Number of criteria
    num_criteria = len(criteria)

    # Compute angle for each axis
    angles = np.linspace(0, 2 * np.pi, num_criteria, endpoint=False).tolist()

    # The plot is a circle, so we need to "complete the loop" and append the start value to the end.
    ratings_array = np.concatenate((ratings_array, ratings_array[:, [0]]), axis=1)
    angles += angles[:1]

    # Plot
    with Params() as params:
        fig, _ax = subplots(scale=1, subplot_kw=dict(polar=True))
        ax: plt.Axes = _ax
        line_styles = ['-', '--', '-.', ':'] * ((len(formats) // 4) + 1)
        for i, (fmt, rating) in enumerate(zip(formats, ratings_array)):
            ax.plot(angles, rating, label=fmt, linestyle=line_styles[i % len(line_styles)], color='black', alpha=0.8)
        ax.fill(angles, ratings_array.mean(axis=0), 'gray', alpha=0.1)

        ax.set_yticklabels([])
        ax.set_thetagrids(np.degrees(angles[:-1]), criteria)
        # plt.legend(loc='upper right', bbox_to_anchor=(1.1, 1.1))
        ax.legend(loc='center left', bbox_to_anchor=(1.15, 0.5))

        if filename:
            filename = pathlib.Path(filename)
            assert filename.suffix in ('.png', '.svg'), "Filename must end with .png or .svg"
            fig.savefig(filename)
            if pdftex and filename.suffix == '.svg':
                svg2pdftex(filename.with_suffix('.svg'))

    return fig, ax
