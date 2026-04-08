"""
astar.py — A* pathfinding with Manhattan, Euclidean, and Chebyshev heuristics.

Features
--------
* ``Grid`` class: randomised walls, weighted terrain costs.
* ``AStar`` class: configurable heuristic, returns path/visited/cost.
* Weighted cells via a ``terrain_cost`` dict mapping ``(row, col)`` → int.
"""

from __future__ import annotations

import heapq
import math
from dataclasses import dataclass, field
from typing import Callable

import numpy as np


# ---------------------------------------------------------------------------
# Grid
# ---------------------------------------------------------------------------

class Grid:
    """2-D grid with random walls and optional terrain costs.

    Parameters
    ----------
    width, height:
        Grid dimensions (columns, rows).
    wall_density:
        Probability that any cell is a wall.
    seed:
        Random seed for reproducibility.
    terrain_cost:
        Optional dict mapping ``(row, col)`` → traversal cost (default 1).
    """

    def __init__(
        self,
        width: int = 20,
        height: int = 20,
        wall_density: float = 0.20,
        seed: int = 42,
        terrain_cost: dict[tuple[int, int], int] | None = None,
    ) -> None:
        self.width  = width
        self.height = height
        rng = np.random.default_rng(seed)
        self._walls = rng.random((height, width)) < wall_density
        # Start and goal must always be passable
        self._walls[0, 0] = False
        self._walls[height - 1, width - 1] = False
        self.terrain_cost: dict[tuple[int, int], int] = terrain_cost or {}

    def is_wall(self, row: int, col: int) -> bool:
        """Return True if ``(row, col)`` is a wall or out of bounds."""
        if row < 0 or row >= self.height or col < 0 or col >= self.width:
            return True
        return bool(self._walls[row, col])

    def passable(self, row: int, col: int) -> bool:
        """Return True if the cell is in-bounds and not a wall."""
        return not self.is_wall(row, col)

    def cost(self, row: int, col: int) -> int:
        """Return the traversal cost of ``(row, col)`` (default 1)."""
        return self.terrain_cost.get((row, col), 1)

    def neighbours(self, row: int, col: int) -> list[tuple[int, int]]:
        """Return 4-directional passable neighbours of ``(row, col)``."""
        candidates = [
            (row - 1, col), (row + 1, col),
            (row, col - 1), (row, col + 1),
        ]
        return [(r, c) for r, c in candidates if self.passable(r, c)]


# ---------------------------------------------------------------------------
# Node
# ---------------------------------------------------------------------------

@dataclass
class _Node:
    """Internal search node for the A* priority queue."""
    f: float
    g: float
    row: int
    col: int
    parent: "_Node | None" = field(default=None, compare=False, repr=False)

    def __lt__(self, other: "_Node") -> bool:
        return self.f < other.f


# ---------------------------------------------------------------------------
# A* solver
# ---------------------------------------------------------------------------

class AStar:
    """A* pathfinding solver.

    Parameters
    ----------
    grid:
        The ``Grid`` to search.
    heuristic:
        One of ``"manhattan"``, ``"euclidean"``, ``"chebyshev"``.
    """

    _HEURISTICS: dict[str, Callable[[int, int, int, int], float]] = {
        "manhattan":  lambda r1, c1, r2, c2: float(abs(r1 - r2) + abs(c1 - c2)),
        "euclidean":  lambda r1, c1, r2, c2: math.sqrt((r1 - r2) ** 2 + (c1 - c2) ** 2),
        "chebyshev":  lambda r1, c1, r2, c2: float(max(abs(r1 - r2), abs(c1 - c2))),
    }

    def __init__(self, grid: Grid, heuristic: str = "manhattan") -> None:
        if heuristic not in self._HEURISTICS:
            raise ValueError(
                f"Unknown heuristic '{heuristic}'. "
                f"Choose from: {list(self._HEURISTICS)}"
            )
        self.grid = grid
        self._h: Callable[[int, int, int, int], float] = self._HEURISTICS[heuristic]

    def solve(
        self,
        start: tuple[int, int],
        end: tuple[int, int],
    ) -> tuple[list[tuple[int, int]], set[tuple[int, int]], float]:
        """Find the lowest-cost path from ``start`` to ``end``.

        Parameters
        ----------
        start:
            ``(row, col)`` of the starting cell.
        end:
            ``(row, col)`` of the target cell.

        Returns
        -------
        path:
            Ordered list of cells from start to end; empty if no path found.
        visited:
            Set of all cells expanded during the search.
        cost:
            Total terrain cost of the path (``inf`` if no path found).
        """
        er, ec = end
        if not self.grid.passable(*start):
            raise ValueError(f"Start cell {start} is a wall.")
        if not self.grid.passable(*end):
            raise ValueError(f"End cell {end} is a wall.")

        open_heap: list[tuple[float, _Node]] = []
        g_best: dict[tuple[int, int], float] = {start: 0.0}
        closed: set[tuple[int, int]] = set()

        sr, sc = start
        h0 = self._h(sr, sc, er, ec)
        heapq.heappush(open_heap, (h0, _Node(f=h0, g=0.0, row=sr, col=sc)))

        while open_heap:
            _, node = heapq.heappop(open_heap)
            pos = (node.row, node.col)
            if pos in closed:
                continue
            closed.add(pos)

            if pos == (er, ec):
                return _reconstruct(node), closed, node.g

            for nr, nc in self.grid.neighbours(node.row, node.col):
                if (nr, nc) in closed:
                    continue
                tentative_g = node.g + self.grid.cost(nr, nc)
                if tentative_g < g_best.get((nr, nc), math.inf):
                    g_best[(nr, nc)] = tentative_g
                    h = self._h(nr, nc, er, ec)
                    nb = _Node(
                        f=tentative_g + h,
                        g=tentative_g,
                        row=nr, col=nc,
                        parent=node,
                    )
                    heapq.heappush(open_heap, (nb.f, nb))

        return [], closed, math.inf


def _reconstruct(node: _Node) -> list[tuple[int, int]]:
    """Walk parent pointers from goal to start and reverse."""
    path: list[tuple[int, int]] = []
    cur: _Node | None = node
    while cur is not None:
        path.append((cur.row, cur.col))
        cur = cur.parent
    path.reverse()
    return path
