import argparse
import os
from collections import defaultdict
from functools import reduce

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import yaml


class UnsuportedOutputFileType(Exception): ...  # noqa


def pre_process_yaml_file(filename, pre_process_config):
    """
    filename should contain one or more entries
    conforming to the following format

    errors: ''
    pass: true
    operation1:
      test_name:
        elapsed:
        start:
        stop:
    operation2:
      ...
    """
    all_data = defaultdict(dict)
    with open(filename, "r") as file:
        data = yaml.safe_load_all(file)

        for data_entry in data:
            if pre_process_config["assert_pass"]:
                assert data_entry["pass"], data_entry["errors"]
            del data_entry["errors"]
            del data_entry["pass"]

            for k, v in data_entry.items():
                for b in v.keys():
                    v[b] = [v[b]["elapsed"]]

            for operation in data_entry:
                for test_name in data_entry[operation]:
                    name = test_name.replace(pre_process_config["remove_test_prefix"], "")
                    if name in all_data[operation]:
                        all_data[operation][name].extend(data_entry[operation][test_name])
                    else:
                        all_data[operation][name] = data_entry[operation][test_name]
    return all_data


def get_test_names(data):
    return reduce(lambda x, y: x | y, [set(v.keys()) for v in data.values()])


def add_combined_operations(data, name, operations_to_combine):
    for test_name in get_test_names(data=data):
        data[name][test_name] = [0] * len(data[operations_to_combine[0]][test_name])
        for operation in operations_to_combine:
            data[name][test_name] = list(pd.Series(data[name][test_name]).add(pd.Series(data[operation][test_name])))


def build_data_frames(data):
    mean_data = defaultdict(dict)
    percentile_99_data = defaultdict(dict)
    for operation in data:
        for name in data[operation]:
            mean_data[operation][name] = np.mean(data[operation][name])
            percentile_99_data[operation][name] = np.percentile(data[operation][name], 99)

    return {
        "mean": pd.DataFrame(mean_data),
        "99pctl": pd.DataFrame(percentile_99_data),
    }


def prepare_plot_axes(axes, frames, row_order, column_order, grid_config, plot_config):
    for idx, title in enumerate(frames):
        frames[title] = frames[title].reindex(row_order[::-1])
        frames[title] = frames[title].reindex(column_order[::-1], axis="columns")
        frames[title].plot(kind=plot_config["kind"], ax=axes[idx], title=title)
        axes[idx].minorticks_on()
        axes[idx].grid(**grid_config)


def prepare_fig_legend(fig, axes, column_order, legend_config):
    handles, labels = plt.gca().get_legend_handles_labels()
    label_dict = {v: k for k, v in enumerate(labels)}
    order = [label_dict[k] for k in column_order]
    legend = ([handles[idx] for idx in order], [labels[idx] for idx in order])
    axes[0].get_legend().remove()
    axes[1].get_legend().remove()
    fig.legend(*legend, loc=legend_config["loc"])


def load_config(filename):
    with open(filename, "r") as file:
        return yaml.safe_load(file)


def output_file(fig, output_config, filename):
    output_backends = {
        ".svg": "svg",
        ".pdf": "pdf",
        ".png": "agg",
    }
    file_extension = filename[filename.rfind(".") :]  # noqa
    if file_extension in output_backends:
        mpl.use(backend=output_backends[file_extension])
    else:
        raise UnsuportedOutputFileType(f"File type not supported: {os.path.basename(filename)}")

    fig.set_dpi(val=output_config["dpi"])
    fig.set_size_inches(w=output_config["width"], h=output_config["height"])
    mpl.pyplot.savefig(fname=filename)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, default="config.yaml")
    parser.add_argument("--output", type=str)
    parser.add_argument("filename", type=str)
    args = parser.parse_args()

    config = load_config(filename=args.config)
    filename = os.path.abspath(os.path.realpath(args.filename))

    all_data = pre_process_yaml_file(filename=filename, pre_process_config=config["pre_process"])

    for operation_name in config.get("combined_operations", {}):
        add_combined_operations(
            data=all_data, name=operation_name, operations_to_combine=config["combined_operations"][operation_name]
        )

    frames = build_data_frames(data=all_data)

    fig, axes = plt.subplots(nrows=len(frames), ncols=1, layout="constrained")

    prepare_plot_axes(
        axes=axes,
        frames=frames,
        row_order=config["test_order"],
        column_order=config["operation_order"],
        grid_config=config["grid"],
        plot_config=config["plot"],
    )
    prepare_fig_legend(
        fig=fig, axes=axes, column_order=config["operation_order"], legend_config=config["legend_config"]
    )

    dataset_count_config = config["dataset_count"]
    fig_data = dict(
        dataset_count={
            field: len(all_data[dataset_count_config["operation"]][field]) for field in dataset_count_config["datasets"]
        },
    )

    for fig_text in config["fig_text"]:
        fig.text(x=fig_text["x"], y=fig_text["y"], s=fig_text["text"].format(**fig_data), fontsize=fig_text["fontsize"])

    if args.output:
        output_filename = os.path.abspath(os.path.realpath(args.output))
        output_file(fig=fig, output_config=config["file_output"], filename=output_filename)
    else:
        mpl.pyplot.ion()
        plt.show(block=True)
        mpl.pyplot.ioff()


if __name__ == "__main__":
    main()
