"""Helper functions used in the PUDL scrapers."""

import datetime


def new_output_dir(root):
    """Produce the name of a fresh output directory.

    Args:
        root (Path): the base directory which may contain previously used output
            directories

    Returns:
        str, A fresh output directory name, such as <root>/2019-05-31#004 where
        004 indicates 3 previous runs exist.
    """
    today = datetime.date.today()
    fp = root.glob(f"{today.isoformat()}*")
    todays_outputs = sorted(fp, reverse=True)

    if todays_outputs == []:
        return root / (f"{today.isoformat()}#{1:03d}")

    previous_name = str(todays_outputs[0])
    previous_number = int(previous_name[-3:])
    return root / (f"{today.isoformat()}#{previous_number + 1:03d}")


def get_latest_directory(root):
    """Get the most recently created output directory for desired dataset.

    This is meant for FERC XBRL and DBF data to be stored in the same directory. This
    solution is slightly hacky, but will be replaced in upcoming refactor.

    Args:
        root (Path): the base directory which may contain previously used output
            directories

    Returns:
        str, A fresh output directory name, such as <root>/2019-05-31#004 where
        004 indicates 3 previous runs exist.
    """
    today = datetime.date.today()
    fp = root.glob(f"{today.isoformat()}*")
    todays_outputs = sorted(fp, reverse=True)

    if todays_outputs == []:
        return root / (f"{today.isoformat()}#{1:03d}")

    previous_name = str(todays_outputs[0])
    previous_number = int(previous_name[-3:])
    return root / (f"{today.isoformat()}#{previous_number:03d}")
