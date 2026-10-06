"""Python entry point: ``python -m hub`` prints the SDK version."""

from . import __version__


def main():
    print("musubi-hub-sdk %s" % __version__)


if __name__ == "__main__":
    main()