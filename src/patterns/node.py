# -*- coding: utf-8 -*-

from __future__ import annotations
from typing import TypeVar

T = TypeVar('T', bound='Node')


class Node:
    def __init__(self, value: int, children: list[T] = []) -> None:
        self.value = value
        self.children = children

    def add_child(self, child: T) -> None:
        self.children.append(child)  # type: ignore[arg-type]


if __name__ == '__main__':
    root = Node(1)
    child = Node(2)
    root.add_child(child)
    print(root.children[0].value)
