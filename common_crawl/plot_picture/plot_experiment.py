"""
Author: Zhan Su
Date: 2022-04-21 22:37:22
LastEditTime: 2022-04-21 22:37:23
LastEditors: Zhan Su
Description: plot the picture in the RQ2 experiment
FilePath: /common_crawl/plot_picture/plot_experiment.py
"""

import plotly.graph_objects as go
import pandas as pd

x_base = [
    "2012",
    "2013",
    "2014",
    "2015",
    "2016",
    "2017",
    "2018",
    "2019",
    "2020",
    "2021",
]


def plot_average_change_rate():
    """plot average num change in RQ2, see the Figure 3"""
    from stastics_cal import trackers_average_edu, trackers_average_base

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=x_base,
            y=trackers_average_base,
            mode="lines",
            name="no-edu websites",
            connectgaps=True,
        )
    )

    fig.add_trace(
        go.Scatter(
            x=x_base,
            y=trackers_average_edu,
            mode="lines",
            name="edu websites",
            connectgaps=True,
        )
    )

    fig.update_layout(legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01))
    fig.update_layout(
        legend=dict(
            yanchor="bottom",
            y=0.01,
            xanchor="right",
            x=0.99,
        )
    )
    fig.update_layout(
        margin=dict(l=10, r=10, b=5, t=5, pad=4)
        # paper_bgcolor="LightSteelBlue",
    )

    fig.update_layout(
        # title="Plot Title",
        # xaxis_title="X Axis Title",
        # yaxis_title="Y Axis Title",
        # legend_title="Legend Title",
        font=dict(
            # family="Arial Black",
            size=18,
            # color="RebeccaPurple"
        )
    )

    fig.write_image("images/third_party_change_compare.pdf")
    fig.show()
    fig.write_image("images/third_party_change_compare.pdf")


########################### plot the rate change of edu and non-edu #######


def plot_rate_change_of_edu_non_edu():
    """rate change of RQ2, Figure 4"""
    fig = go.Figure()
    df_rate_merge_edu = pd.read_csv("dataset_archive/df_rate_merge_edu.csv")
    df_rate_merge_base = pd.read_csv("dataset_archive/df_rate_merge_base.csv")
    # df_rate_merge_edu = df_rate_merge_edu.drop([4])
    # df_rate_merge_base = df_rate_merge_base.drop([4, 5, 6, 7])

    print(df_rate_merge_edu)
    for _, row in df_rate_merge_edu[:5].iterrows():
        y_list = [row[i] for i in x_base]
        x_draw = [
            "2012",
            "2013",
            "2014",
            "2015",
            "2016",
            "2017",
            "2018",
            "2019",
            "2020",
            "2021",
        ]

        fig.add_trace(
            go.Line(x=x_draw, y=y_list, name=row["trackers"] + "(edu)")
            # row = 2,col = 1
        )
    for _, row in df_rate_merge_base[:5].iterrows():
        y_list = [row[i] for i in x_base]
        x_draw = [
            "2012",
            "2013",
            "2014",
            "2015",
            "2016",
            "2017",
            "2018",
            "2019",
            "2020",
            "2021",
        ]

        fig.add_trace(
            go.Scatter(
                x=x_draw,
                y=y_list,
                name=row["trackers"] + "(no_edu)",
                line=dict(width=4, dash="dot"),
            )
            # row = 2,col = 1
        )
    fig.update_layout(
        margin=dict(l=10, r=10, b=5, t=5, pad=4)
        # paper_bgcolor="LightSteelBlue",
    )

    # fig.update_layout(
    #     # title="Plot Title",
    #     # xaxis_title="X Axis Title",
    #     # yaxis_title="Y Axis Title",
    #     # legend_title="Legend Title",
    #     font=dict(
    #         # family="Arial Black",
    #         size=18,
    #         # color="RebeccaPurple"
    #     )
    # )
    fig.write_image("images/third_party_rate_change_comparision.pdf")
    fig.show()
    fig.write_image("images/third_party_rate_change_comparision.pdf")


# plot_average_change_rate()
plot_rate_change_of_edu_non_edu()
