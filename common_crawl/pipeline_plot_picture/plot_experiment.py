"""
Author: Zhan Su
Date: 2022-04-21 22:37:22
LastEditTime: 2022-04-21 22:37:23
LastEditors: Zhan Su
Description: plot the picture in the RQ2 experiment
FilePath: /common_crawl/plot_picture/plot_experiment.py

resource:

https://plotly.com/python/continuous-error-bars/
"""

from turtle import fillcolor
import plotly.graph_objects as go
import pandas as pd
import plotly.express as px
from plotly.subplots import make_subplots
import numpy as np

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
    # from stastics_cal import trackers_average_edu, trackers_average_base

    trackers_average_base = pd.read_csv(
        "dataset_archive/frame_control_count.csv", sep="\t"
    )
    trackers_average_edu = pd.read_csv("dataset_archive/frame_edu_count.csv", sep="\t")

    fig = go.Figure()

    fig.add_trace(
        go.Line(
            x=x_base,
            y=np.log(trackers_average_base.mean()),
            name="no-edu websites mean",
            text="hello",
        )
    )

    # fig.add_trace(
    #     go.Line(
    #         name="no-edu websites std",
    #         x=x_base,
    #         y=trackers_average_base.std(),
    #     )
    # )

    fig.add_trace(
        go.Line(
            x=x_base,
            y=np.log(trackers_average_edu.mean()),
            name="edu websites mean",
        )
    )

    # fig.add_trace(
    #     go.Line(
    #         name="edu websites std",
    #         x=x_base,
    #         y=trackers_average_edu.std(),
    #     )
    # )

    fig.update_layout(legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01))
    fig.update_layout(
        legend=dict(
            yanchor="bottom",
            y=0.01,
            xanchor="right",
            x=0.99,
        )
    )

    # large_rockwell_template = dict(
    #     layout=go.Layout(title_font=dict(family="Rockwell", size=24))
    # )

    fig.update_layout(
        margin=dict(l=10, r=10, b=5, t=5, pad=4),
        # paper_bgcolor="rgba(0,0,0,0)",
        # plot_bgcolor="rgba(0,0,0,0)"
        # template=large_rockwell_template,
    )

    # fig.update_xaxes(showline=True, linewidth=2, linecolor="black", mirror=True)
    # fig.update_yaxes(showline=True, linewidth=2, linecolor="black", mirror=True)
    # fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor="black")
    # fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor="black")
    fig.update_layout(
        # title="Plot Title",
        xaxis_title="Year",
        yaxis_title="Average number of trackers",
        # legend_title="Legend Title",
        font=dict(
            # family="Arial Black",
            size=18,
            # color="RebeccaPurple"
        ),
    )

    fig.write_image("images/third_party_change_compare.pdf")
    fig.show()
    fig.write_image("images/third_party_change_compare.pdf")


########################### plot the rate change of edu and non-edu #######


def plot_rate_change_of_edu_non_edu():
    """rate change of RQ2, Figure 4"""

    color_list = px.colors.qualitative.Plotly
    tracker_name = ["google-analytics", "facebook", "twitter", "google", "youtube"]
    color_dict = dict(zip(tracker_name, color_list))
    fig = go.Figure()
    df_rate_merge_edu = pd.read_csv("dataset_archive/df_rate_merge_edu.csv")
    df_rate_merge_base = pd.read_csv("dataset_archive/df_rate_merge_base.csv")
    # df_rate_merge_edu = df_rate_merge_edu.drop([4])
    # df_rate_merge_base = df_rate_merge_base.drop([4, 5, 6, 7])

    for e, row in df_rate_merge_edu[:5].iterrows():
        y_list = [row[i] for i in x_base]
        print(color_list[e])
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
                name=row["trackers"] + "(edu)",
                mode="lines+markers",
                line=dict(color=color_dict[row["trackers"]]),
            ),
            # row = 2,col = 1
        )
    for e, row in df_rate_merge_base[:5].iterrows():
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
                line=dict(width=4, dash="dot", color=color_dict[row["trackers"]]),
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
# plot_rate_change_of_edu_non_edu()


def plot_test():
    x = [
        "day 1",
        "day 1",
        "day 1",
        "day 1",
        "day 1",
        "day 1",
        "day 2",
        "day 2",
        "day 2",
        "day 2",
        "day 2",
        "day 2",
    ]

    x = [1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2]
    fig = go.Figure()

    fig.add_trace(
        go.Box(
            y=[0.2, 0.2, 0.6, 1.0, 0.5, 0.4, 0.2, 0.7, 0.9, 0.1, 0.5, 0.3],
            x=x,
            marker_color="#3D9970",
        )
    )
    fig.add_trace(
        go.Box(
            y=[0.6, 0.7, 0.3, 0.6, 0.0, 0.5, 0.7, 0.9, 0.5, 0.8, 0.7, 0.2],
            x=x,
            marker_color="#FF4136",
        )
    )
    fig.add_trace(
        go.Box(
            y=[0.1, 0.3, 0.1, 0.9, 0.6, 0.6, 0.9, 1.0, 0.3, 0.6, 0.8, 0.5],
            x=x,
            marker_color="#FF851B",
        )
    )

    fig.update_layout(
        yaxis_title="normalized moisture",
        boxmode="group",  # group together boxes of the different traces for each value of x
    )
    fig.show()


def plot_median_min_max():

    from stastics_cal import plot_get_df_count_all_year

    df_count_all_year_edu, df_count_all_year_base = plot_get_df_count_all_year()
    # print(df_count_all_year_base.loc[df_count_all_year_base.trackers > 40])
    # df = pd.concat([df_count_all_year_edu, df_count_all_year_base])
    fig = go.Figure()

    # fig = make_subplots(rows=5, cols=2, start_cell="bottom-left")
    # for e, year in enumerate(x_base):

    #     tracker_2012_edu = df_count_all_year_edu.loc[
    #         df_count_all_year_edu.year == year, "trackers"
    #     ]

    #     tracker_2012_base = df_count_all_year_base.loc[
    #         df_count_all_year_base.year == year, "trackers"
    #     ]

    #     fig.add_trace(
    #         go.Box(
    #             y=np.log(tracker_2012_edu),
    #             boxpoints=False,
    #             name="edu websites",
    #             boxmean="sd",
    #         ),
    #         row=int(e / 2) + 1,
    #         col=1,
    #     )
    #     fig.add_trace(
    #         go.Box(
    #             y=np.log(tracker_2012_base),
    #             boxpoints=False,
    #             name="control websites",
    #             boxmean="sd",
    #         ),
    #         row=int(e / 2) + 1,
    #         col=1,
    #     )
    fig.add_trace(
        go.Box(
            x=df_count_all_year_edu["year"],
            y=np.log(df_count_all_year_edu["trackers"]),
            name="edu websites",
            boxmean="sd",
        )
    )
    fig.add_trace(
        go.Box(
            x=df_count_all_year_base["year"],
            y=np.log(df_count_all_year_base["trackers"]),
            name="non-edu websites",
            boxmean="sd",
        )
    )
    fig.update_layout(
        title="Min,Max and median, std of trackers for edu and non-edu websites",
        # boxmode="group",  # group together boxes of the different traces for each value of x
    )

    fig.update_layout(
        margin=dict(l=10, r=10, b=10, t=30, pad=5),
        # paper_bgcolor="rgba(0,0,0,0)",
        # plot_bgcolor="rgba(0,0,0,0)",
    )
    fig.write_image("images/median_min_max.pdf")
    fig.show()
    fig.write_image("images/median_min_max.pdf")


# plot_average_change_rate()
# plot_median_min_max()
# plot_test()
plot_rate_change_of_edu_non_edu()
