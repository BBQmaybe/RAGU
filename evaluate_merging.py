#!/usr/bin/env python3
"""
Оценка качества слияния вершин графа знаний.

Скрипт сравнивает два GML-графа: эталонный (benchmark) и построенный методом (predicted).
Для каждой пары вершин определяет TP/FP/FN/TN и вычисляет Precision, Recall, Accuracy, F1.

Принцип работы:
  Вершины идентифицируются по атрибуту `label` (уникальный ID сущности).
  Кластер (группа слитых вершин) определяется по атрибуту `entity_name`:
  вершины с одинаковым entity_name считаются слитыми в одну сущность.

  Для каждой пары вершин, присутствующих в обоих графах:
    TP — пара слита в обоих графах (одинаковый entity_name)
    FP — пара слита в predicted, но не в benchmark
    FN — пара слита в benchmark, но не в predicted
    TN — пара не слита ни в одном из графов
"""

import argparse
import sys
from itertools import combinations

import networkx as nx


def load_clusters(path: str) -> dict[str, str]:
    """Загружает граф из GML и возвращает mapping: label -> entity_name (нормализованный)."""
    graph = nx.read_gml(path)
    clusters: dict[str, str] = {}
    for node, data in graph.nodes(data=True):
        entity_name = data.get("entity_name", str(node))
        clusters[str(node)] = entity_name.strip().lower()
    return clusters


def evaluate(benchmark_path: str, predicted_path: str) -> None:
    bench = load_clusters(benchmark_path)
    pred = load_clusters(predicted_path)

    common = sorted(set(bench) & set(pred))

    if len(common) < 2:
        print(f"Недостаточно общих вершин для сравнения: {len(common)}", file=sys.stderr)
        sys.exit(1)

    tp = fp = fn = tn = 0

    for a, b in combinations(common, 2):
        same_bench = bench[a] == bench[b]
        same_pred = pred[a] == pred[b]

        if same_bench and same_pred:
            tp += 1
        elif same_pred and not same_bench:
            fp += 1
        elif same_bench and not same_pred:
            fn += 1
        else:
            tn += 1

    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    accuracy = (tp + tn) / (tp + fp + fn + tn) if (tp + fp + fn + tn) else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0

    print(f"TP: {tp}")
    print(f"FP: {fp}")
    print(f"FN: {fn}")
    print(f"TN: {tn}")
    print(f"Precision: {precision:.4f}")
    print(f"Recall: {recall:.4f}")
    print(f"Accuracy: {accuracy:.4f}")
    print(f"F1: {f1:.4f}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Оценка качества слияния вершин графа знаний"
    )
    parser.add_argument(
        "--benchmark",
        required=True,
        help="Путь к эталонному GML-графу (ground truth)",
    )
    parser.add_argument(
        "--predicted",
        required=True,
        help="Путь к GML-графу, построенному оцениваемым методом",
    )
    args = parser.parse_args()
    evaluate(args.benchmark, args.predicted)


if __name__ == "__main__":
    main()
