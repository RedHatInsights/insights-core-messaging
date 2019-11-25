#!/usr/bin/env python
import argparse
from insights_messaging.builder import AppBuilder


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("config", help="Application Configuration.")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    with open(args.config) as f:
        AppBuilder(f.read()).build_app().run()
