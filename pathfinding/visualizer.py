"""
visualizer.py — ASCII (rich) and matplotlib visualisers for A* results.

Legend
------
  S  start            E  end
  #  wall             *  optimal path
  .  visited cell     (space)  open/unvisited
"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from pathfinding.astar import Grid, AStar
from config import GRID_SIZE, WALL_DENSITY, GRID_SEED, ASTAR_PNG


def ascii_visualize(
    grid: Grid,
    path: list[tuple[int, int]],
    visited: set[tuple[int, int]],
    start: tuple[int, int],
    end: tuple[int, int],
) -> None:
    """Print a colour-coded terminal representation of the solved maze.

    Uses ``rich`` for colour if available; falls back to plain ASCII.

    Parameters
    ----------
    grid:
        The solved grid.
    path:
        Cells on the optimal path.
    visited:
        All cells expanded during search.
    start, end:
        Start and end coordinates.
    """
    path_set = set(path)
    rows: list[str] = []
    for r in range(grid.height):
        row_chars: list[str] = []
        for c in range(grid.width):
            if (r, c) == start:
                row_chars.append("S")
            elif (r, c) == end:
                row_chars.append("E")
            elif grid.is_wall(r, c):
                row_chars.append("#")
            elif (r, c) in path_set:
                row_chars.append("*")
            elif (r, c) in visited:
                row_chars.append(".")
            else:
                row_chars.append(" ")
        rows.append(" ".join(row_chars))

    header = (
        f"  Grid {grid.height}×{grid.width}  |  "
        f"Path: {len(path)} steps  |  "
        f"Visited: {len(visited)} cells  |  "
        f"{'PATH FOUND' if path else 'NO PATH'}"
    )

    try:
        from rich.console import Console
        from rich.text import Text
        console = Console()
        console.print(f"\n[bold]{header}[/bold]\n")
        for line in rows:
            t = Text()
            for ch in line:
                if ch == "S":
                    t.append(ch, style="bold green")
                elif ch == "E":
                    t.append(ch, style="bold red")
                elif ch == "#":
                    t.append(ch, style="bright_black")
                elif ch == "*":
                    t.append(ch, style="bold yellow")
                elif ch == ".":
                    t.append(ch, style="cyan")
                else:
                    t.append(ch)
            console.print(t)
    except ImportError:
        print(f"\n{header}\n")
        for line in rows:
            print(line)


def plot_visualize(
    grid: Grid,
    path: list[tuple[int, int]],
    visited: set[tuple[int, int]],
    start: tuple[int, int],
    end: tuple[int, int],
    save_path: Path = ASTAR_PNG,
) -> None:
    """Save a colour-coded matplotlib figure of the solved maze.

    Colour key
    ----------
    White=open, dark grey=wall, light blue=visited, yellow=path,
    green=start, red=end.

    Parameters
    ----------
    save_path:
        Output PNG file path.
    """
    try:
        import matplotlib.pyplot as plt
        import matplotlib.colors as mc
        import matplotlib.patches as mpatches
        import numpy as np
    except ImportError:
        print("  matplotlib not installed — skipping PNG visualisation.")
        return

    canvas = np.zeros((grid.height, grid.width), dtype=int)
    for r in range(grid.height):
        for c in range(grid.width):
            if grid.is_wall(r, c):
                canvas[r, c] = 1
    for r, c in visited:
        if canvas[r, c] == 0:
            canvas[r, c] = 2
    for r, c in path:
        canvas[r, c] = 3
    canvas[start[0], start[1]] = 4
    canvas[end[0],   end[1]]   = 5

    cmap = mc.ListedColormap([
        "#f5f5f5",  # 0 open
        "#2d2d2d",  # 1 wall
        "#aed6f1",  # 2 visited
        "#f9e79f",  # 3 path
        "#2ecc71",  # 4 start
        "#e74c3c",  # 5 end
    ])
    norm = mc.BoundaryNorm([-0.5, 0.5, 1.5, 2.5, 3.5, 4.5, 5.5], cmap.N)

    fig, ax = plt.subplots(figsize=(8, 8))
    ax.imshow(canvas, cmap=cmap, norm=norm, origin="upper")
    ax.set_title(
        f"A* Pathfinding — {grid.height}×{grid.width} grid\n"
        f"Path: {len(path)} steps  |  Visited: {len(visited)} cells",
        fontsize=10,
    )
    ax.set_xticks([])
    ax.set_yticks([])
    handles = [
        mpatches.Patch(color="#f5f5f5", label="Open",    edgecolor="#ccc"),
        mpatches.Patch(color="#2d2d2d", label="Wall"),
        mpatches.Patch(color="#aed6f1", label="Visited"),
        mpatches.Patch(color="#f9e79f", label="Path"),
        mpatches.Patch(color="#2ecc71", label="Start"),
        mpatches.Patch(color="#e74c3c", label="End"),
    ]
    ax.legend(handles=handles, loc="upper right",
              bbox_to_anchor=(1.22, 1.0), fontsize=8)
    plt.tight_layout()
    save_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(save_path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved matplotlib figure → {save_path}")


def demo(
    size: int = GRID_SIZE,
    wall_density: float = WALL_DENSITY,
    seed: int = GRID_SEED,
) -> dict:
    """Create a maze, solve it with A*, and run both visualisers.

    Parameters
    ----------
    size:
        Grid width = height (default 20).
    wall_density:
        Fraction of cells that are walls.
    seed:
        Reproducibility seed.

    Returns
    -------
    dict with keys: path, visited, cost, grid.
    """
    grid   = Grid(width=size, height=size, wall_density=wall_density, seed=seed)
    solver = AStar(grid, heuristic="manhattan")
    start  = (0, 0)
    end    = (size - 1, size - 1)
    path, visited, cost = solver.solve(start, end)

    ascii_visualize(grid, path, visited, start, end)

    if path:
        print(f"\n  Path cost    : {cost:.1f}")
        print(f"  Path length  : {len(path)} steps")
        print(f"  Nodes visited: {len(visited)}")
    else:
        print("\n  No path found — reduce wall_density in config.py")

    plot_visualize(grid, path, visited, start, end)

    return {"path": path, "visited": visited, "cost": cost, "grid": grid}
