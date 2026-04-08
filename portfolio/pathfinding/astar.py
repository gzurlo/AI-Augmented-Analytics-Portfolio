"""
astar.py — Full A* pathfinding implementation with heuristic options and
weighted terrain costs.

Algorithm overview
------------------
A* explores a grid from a *start* node to a *goal* node by maintaining an
open priority queue.  For each candidate node ``n`` it computes::

    f(n) = g(n) + h(n)

where ``g(n)`` is the exact cost from the start and ``h(n)`` is an admissible
heuristic estimate of the remaining cost.  Nodes are expanded in order of
lowest ``f``.

This implementation supports:
* **Manhattan** heuristic — optimal for 4-directional grids.
* **Euclidean** heuristic — optimal for 8-directional (diagonal) grids.
* **Weighted grids** — each non-wall cell may carry an integer terrain cost.
* Returns the path, the set of visited nodes, and the total path cost.
"""

from __future__ import annotations

import heapq
import math
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable

import numpy as np


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

class HeuristicType(Enum):
    """Available heuristic functions for A*."""
    MANHATTAN = "manhattan"
    EUCLIDEAN = "euclidean"


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class Node:
    """A single cell in the search grid.

    Attributes
    ----------
    row, col:
        Grid coordinates.
    g:
        Cumulative cost from the start node.
    h:
        Heuristic estimate to the goal.
    f:
        ``g + h`` — the priority used in the open queue.
    parent:
        Reference to the previous node on the cheapest path found so far.
    """
    row: int
    col: int
    g: float = field(default=0.0, compare=False)
    h: float = field(default=0.0, compare=False)
    f: float = field(default=0.0, compare=False)
    parent: "Node | None" = field(default=None, compare=False, repr=False)

    def __lt__(self, other: "Node") -> bool:
        """Tie-break on h so nodes closer to goal are preferred."""
        if self.f == other.f:
            return self.h < other.h
        return self.f < other.f


@dataclass
class Grid:
    """2-D grid of cells.

    Attributes
    ----------
    rows, cols:
        Grid dimensions.
    walls:
        Boolean array; ``True`` means the cell is impassable.
    costs:
        Integer terrain costs per cell (default 1 everywhere).
    """
    rows: int
    cols: int
    walls: np.ndarray      # shape (rows, cols), dtype bool
    costs: np.ndarray      # shape (rows, cols), dtype int

    @classmethod
    def random(
        cls,
        rows: int,
        cols: int,
        wall_prob: float = 0.25,
        seed: int = 42,
    ) -> "Grid":
        """Generate a random grid with a given wall probability.

        The start (0, 0) and goal (rows-1, cols-1) are always passable.

        Parameters
        ----------
        rows, cols:
            Grid dimensions.
        wall_prob:
            Probability that any given cell is a wall.
        seed:
            Random seed for reproducibility.
        """
        rng = np.random.default_rng(seed)
        walls = rng.random((rows, cols)) < wall_prob
        walls[0, 0] = False
        walls[rows - 1, cols - 1] = False
        costs = np.ones((rows, cols), dtype=int)
        # Add terrain variation: some cells cost 2 or 3
        terrain = rng.integers(1, 4, size=(rows, cols))
        costs[~walls] = terrain[~walls]
        return cls(rows=rows, cols=cols, walls=walls, costs=costs)

    def is_passable(self, row: int, col: int) -> bool:
        """Return True if the cell exists and is not a wall."""
        return (
            0 <= row < self.rows
            and 0 <= col < self.cols
            and not self.walls[row, col]
        )

    def neighbours(self, row: int, col: int) -> list[tuple[int, int]]:
        """Return passable 4-directional neighbours of (row, col)."""
        candidates = [
            (row - 1, col),  # up
            (row + 1, col),  # down
            (row, col - 1),  # left
            (row, col + 1),  # right
        ]
        return [(r, c) for r, c in candidates if self.is_passable(r, c)]


# ---------------------------------------------------------------------------
# A* solver
# ---------------------------------------------------------------------------

class AStarSolver:
    """Solve a pathfinding problem on a ``Grid`` using the A* algorithm.

    Parameters
    ----------
    grid:
        The search space.
    heuristic:
        Which admissible heuristic to use (default: MANHATTAN).
    """

    def __init__(
        self,
        grid: Grid,
        heuristic: HeuristicType = HeuristicType.MANHATTAN,
    ) -> None:
        self.grid = grid
        self._heuristic_fn: Callable[[int, int, int, int], float] = (
            _manhattan if heuristic == HeuristicType.MANHATTAN else _euclidean
        )

    def solve(
        self,
        start: tuple[int, int],
        goal: tuple[int, int],
    ) -> tuple[list[tuple[int, int]], set[tuple[int, int]], float]:
        """Find the lowest-cost path from *start* to *goal*.

        Parameters
        ----------
        start:
            ``(row, col)`` of the start cell.
        goal:
            ``(row, col)`` of the goal cell.

        Returns
        -------
        path:
            Ordered list of ``(row, col)`` tuples from start to goal.
            Empty list if no path exists.
        visited:
            Set of all ``(row, col)`` nodes expanded during search.
        cost:
            Total terrain cost of the returned path.
        """
        sr, sc = start
        gr, gc = goal

        if not self.grid.is_passable(sr, sc):
            raise ValueError(f"Start cell {start} is a wall or out of bounds.")
        if not self.grid.is_passable(gr, gc):
            raise ValueError(f"Goal cell {goal} is a wall or out of bounds.")

        open_heap: list[tuple[float, Node]] = []
        closed: set[tuple[int, int]] = set()

        start_node = Node(
            row=sr,
            col=sc,
            g=0.0,
            h=self._heuristic_fn(sr, sc, gr, gc),
        )
        start_node.f = start_node.g + start_node.h
        heapq.heappush(open_heap, (start_node.f, start_node))

        # Best known g-cost per cell
        g_best: dict[tuple[int, int], float] = {(sr, sc): 0.0}

        while open_heap:
            _, current = heapq.heappop(open_heap)

            pos = (current.row, current.col)
            if pos in closed:
                continue
            closed.add(pos)

            # Goal reached — reconstruct path
            if pos == (gr, gc):
                path = _reconstruct_path(current)
                return path, closed, current.g

            for nr, nc in self.grid.neighbours(current.row, current.col):
                if (nr, nc) in closed:
                    continue

                step_cost = float(self.grid.costs[nr, nc])
                tentative_g = current.g + step_cost

                if tentative_g < g_best.get((nr, nc), math.inf):
                    g_best[(nr, nc)] = tentative_g
                    h = self._heuristic_fn(nr, nc, gr, gc)
                    neighbour = Node(
                        row=nr,
                        col=nc,
                        g=tentative_g,
                        h=h,
                        f=tentative_g + h,
                        parent=current,
                    )
                    heapq.heappush(open_heap, (neighbour.f, neighbour))

        # No path found
        return [], closed, float("inf")


# ---------------------------------------------------------------------------
# Heuristics
# ---------------------------------------------------------------------------

def _manhattan(r1: int, c1: int, r2: int, c2: int) -> float:
    """Manhattan distance — admissible for 4-directional grids."""
    return float(abs(r1 - r2) + abs(c1 - c2))


def _euclidean(r1: int, c1: int, r2: int, c2: int) -> float:
    """Euclidean distance — admissible for 8-directional grids."""
    return math.sqrt((r1 - r2) ** 2 + (c1 - c2) ** 2)


# ---------------------------------------------------------------------------
# Path reconstruction
# ---------------------------------------------------------------------------

def _reconstruct_path(node: Node) -> list[tuple[int, int]]:
    """Walk parent pointers from goal back to start and reverse."""
    path: list[tuple[int, int]] = []
    current: Node | None = node
    while current is not None:
        path.append((current.row, current.col))
        current = current.parent
    path.reverse()
    return path
