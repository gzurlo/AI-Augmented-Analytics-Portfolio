"""
visualizer.py — ASCII and matplotlib visualisers for A* grid pathfinding.

Two output modes
----------------
* ``visualize_ascii``      — works in any terminal; no dependencies beyond stdlib.
* ``visualize_matplotlib`` — colour-coded PNG / interactive window; requires
  matplotlib (listed in requirements.txt).

Legend
------
::

    S  start
    G  goal
    #  wall
    *  path
    ·  visited (but not on path)
    .  unvisited passable cell
"""

from __future__ import annotations

from pathlib import Path
from typing import Sequence

import numpy as np

from pathfinding.astar import AStarSolver, Grid, HeuristicType
from config import (
    ASTAR_GRID_ROWS,
    ASTAR_GRID_COLS,
    ASTAR_WALL_PROBABILITY,
    ASTAR_RANDOM_SEED,
)


# ---------------------------------------------------------------------------
# ASCII visualiser
# ---------------------------------------------------------------------------

def visualize_ascii(
    grid: Grid,
    path: list[tuple[int, int]],
    visited: set[tuple[int, int]],
    start: tuple[int, int],
    goal: tuple[int, int],
) -> str:
    """Render the grid, path, and visited nodes as an ASCII string.

    Parameters
    ----------
    grid:
        The solved grid.
    path:
        Sequence of ``(row, col)`` tuples forming the optimal path.
    visited:
        Set of cells expanded during A* search.
    start, goal:
        Coordinates of the start and goal cells.

    Returns
    -------
    A multi-line string ready to print to stdout.
    """
    path_set = set(path)
    lines: list[str] = []

    for r in range(grid.rows):
        row_chars: list[str] = []
        for c in range(grid.cols):
            if (r, c) == start:
                row_chars.append("S")
            elif (r, c) == goal:
                row_chars.append("G")
            elif grid.walls[r, c]:
                row_chars.append("#")
            elif (r, c) in path_set:
                row_chars.append("*")
            elif (r, c) in visited:
                row_chars.append("·")
            else:
                row_chars.append(".")
        lines.append(" ".join(row_chars))

    header = (
        f"Grid {grid.rows}×{grid.cols}  |  "
        f"Path length: {len(path)}  |  "
        f"Visited: {len(visited)}  |  "
        f"{'PATH FOUND' if path else 'NO PATH'}"
    )
    return header + "\n\n" + "\n".join(lines)


# ---------------------------------------------------------------------------
# Matplotlib visualiser
# ---------------------------------------------------------------------------

def visualize_matplotlib(
    grid: Grid,
    path: list[tuple[int, int]],
    visited: set[tuple[int, int]],
    start: tuple[int, int],
    goal: tuple[int, int],
    save_path: Path | None = None,
) -> None:
    """Render the grid with matplotlib using a colour-coded heatmap.

    Colour map
    ----------
    * Dark grey  — wall
    * Light blue — visited (expanded) cells
    * Yellow     — optimal path
    * Green      — start
    * Red        — goal
    * White      — unvisited passable cells

    Parameters
    ----------
    grid:
        The solved grid.
    path:
        Sequence of ``(row, col)`` tuples on the optimal path.
    visited:
        Set of expanded cells.
    start, goal:
        Coordinates.
    save_path:
        If given, save the figure to this path instead of showing it.
    """
    try:
        import matplotlib.pyplot as plt
        import matplotlib.colors as mcolors
        import matplotlib.patches as mpatches
    except ImportError:
        print("  matplotlib not installed — skipping graphical visualisation.")
        return

    # Build numeric grid for imshow
    # 0=passable, 1=wall, 2=visited, 3=path, 4=start, 5=goal
    canvas = np.zeros((grid.rows, grid.cols), dtype=int)
    canvas[grid.walls] = 1
    for r, c in visited:
        if canvas[r, c] == 0:
            canvas[r, c] = 2
    for r, c in path:
        canvas[r, c] = 3
    canvas[start[0], start[1]] = 4
    canvas[goal[0], goal[1]] = 5

    cmap = mcolors.ListedColormap([
        "#f5f5f5",  # 0: passable
        "#2d2d2d",  # 1: wall
        "#aed6f1",  # 2: visited
        "#f9e79f",  # 3: path
        "#2ecc71",  # 4: start
        "#e74c3c",  # 5: goal
    ])
    bounds = [-0.5, 0.5, 1.5, 2.5, 3.5, 4.5, 5.5]
    norm = mcolors.BoundaryNorm(bounds, cmap.N)

    fig, ax = plt.subplots(figsize=(9, 9))
    ax.imshow(canvas, cmap=cmap, norm=norm, origin="upper")
    ax.set_title(
        f"A* Pathfinding — {grid.rows}×{grid.cols} grid\n"
        f"Path length: {len(path)}  |  Visited: {len(visited)}",
        fontsize=11,
    )
    ax.set_xticks([])
    ax.set_yticks([])

    legend_handles = [
        mpatches.Patch(color="#f5f5f5", label="Passable", edgecolor="grey"),
        mpatches.Patch(color="#2d2d2d", label="Wall"),
        mpatches.Patch(color="#aed6f1", label="Visited"),
        mpatches.Patch(color="#f9e79f", label="Path"),
        mpatches.Patch(color="#2ecc71", label="Start"),
        mpatches.Patch(color="#e74c3c", label="Goal"),
    ]
    ax.legend(
        handles=legend_handles,
        loc="upper right",
        bbox_to_anchor=(1.18, 1.0),
        fontsize=8,
    )

    plt.tight_layout()
    if save_path:
        save_path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(save_path, dpi=120, bbox_inches="tight")
        print(f"  Matplotlib figure saved to: {save_path}")
    else:
        try:
            plt.show()
        except Exception:
            pass
    plt.close(fig)


# ---------------------------------------------------------------------------
# Demo function
# ---------------------------------------------------------------------------

def demo_maze(
    rows: int = ASTAR_GRID_ROWS,
    cols: int = ASTAR_GRID_COLS,
    wall_prob: float = ASTAR_WALL_PROBABILITY,
    seed: int = ASTAR_RANDOM_SEED,
    save_png: Path | None = None,
) -> dict:
    """Generate a random maze, solve it with A*, and visualise both ways.

    Parameters
    ----------
    rows, cols:
        Grid dimensions (default 20×20 from config).
    wall_prob:
        Probability of any cell being a wall.
    seed:
        Random seed for reproducibility.
    save_png:
        Optional file path for the matplotlib PNG output.

    Returns
    -------
    dict with keys ``path``, ``visited``, ``cost``, ``grid``.
    """
    grid = Grid.random(rows, cols, wall_prob=wall_prob, seed=seed)
    start = (0, 0)
    goal = (rows - 1, cols - 1)

    solver = AStarSolver(grid, heuristic=HeuristicType.MANHATTAN)
    path, visited, cost = solver.solve(start, goal)

    ascii_output = visualize_ascii(grid, path, visited, start, goal)
    print(ascii_output)

    if path:
        print(f"\n  Total path cost : {cost:.1f}")
        print(f"  Nodes visited   : {len(visited)}")
        print(f"  Path length     : {len(path)} steps")
    else:
        print("\n  No path found — try a lower wall probability.")

    visualize_matplotlib(grid, path, visited, start, goal, save_path=save_png)

    return {"path": path, "visited": visited, "cost": cost, "grid": grid}
