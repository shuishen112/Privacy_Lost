"""
Author: your name
Date: 2022-01-07 11:15:34
LastEditTime: 2022-02-15 14:55:27
LastEditors: Please set LastEditors
Description: 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
FilePath: /common_crawl/resource/educational_websites_analyse/plot.py
"""

# 对实验结果进行绘画处理

from tkinter import font
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import matplotlib.pyplot as plt
import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from config import args

tracker_type = args["tracker_type"]
element_type = args["element_type"]
name_dict = {
    "company": f"df_company_rate_compare_{tracker_type}{element_type}.csv",
    "tracker": f"tracker_edu_non_edu_compare_{tracker_type}{element_type}.csv",
}

# print(df_rate)

# fig = make_subplots(rows=1, cols=2,subplot_titles=("The distribution of 3d-party's owner company ","The distribution of 3d-party"))


def plot_figure12(df_rate, draw_type):

    x = df_rate[draw_type]
    y_edu = df_rate["edu"]
    y_control = df_rate["no-edu"]
    plt.rc("font", size="10")
    
    plt.bar(x, y_edu, label="educational websites", color="red")
    plt.bar(x, y_control, label="non-educationl websites", color="blue")
    plt.xticks(rotation=45)
    plt.legend()
    plt.savefig(
        "images/{}_rate_{}{}.png".format(draw_type, tracker_type, element_type),bbox_inches='tight', dpi=200
    )

    plt.show()
    # if draw_type == "company":
    #     color_pool = ["indianred", "lightsalmon"]
    # else:
    #     color_pool = [None, None]
    # fig = go.Figure()
    # fig.add_trace(
    #     go.Bar(
    #         x=df_rate[draw_type],
    #         y=df_rate["edu"],
    #         # orientation='h',
    #         name="educational websites",
    #         # marker_color="indianred",
    #         marker_color=color_pool[0],
    #         text=list(map(lambda x: round(x, 2), df_rate["edu"])),
    #     )
    # )

    # fig.add_trace(
    #     go.Bar(
    #         x=df_rate[draw_type],
    #         y=df_rate["no-edu"],
    #         # orientation='h',
    #         name="no-educational websites",
    #         # marker_color="lightsalmon",
    #         marker_color=color_pool[1],
    #         text=list(map(lambda x: round(x, 2), df_rate["no-edu"])),
    #     )
    # )

    # draw_type = "tracker"
    # df_rate = pd.read_csv("resource/educational_websites_analyse/edu_trackers_rate.csv")[:10]
    # fig.add_trace(go.Bar(
    #     x=df_rate[draw_type],
    #     y=df_rate['edu'],
    #     # orientation='h',
    #     name='educational websites',
    #     # marker_color='indianred',
    #     text = list(map(lambda x:round(x,2),df_rate['edu']))

    # ),row = 1,col = 2)
    # fig.add_trace(go.Bar(
    #     x=df_rate[draw_type],
    #     y=df_rate['no-edu'],
    #     # orientation='h',
    #     name='no-educational websites',
    #     # marker_color='lightsalmon',
    #     text=list(map(lambda x:round(x,2),df_rate['no-edu']))

    # ),row = 1,col = 2)

    # Here we modify the tickangle of the xaxis, resulting in rotated labels.
    # fig.update_layout(
    #     barmode="group",
    #     xaxis_tickangle=-45,
    #     title_text="",
    # )
    # fig.update_layout(
    #     legend=dict(
    #         yanchor="top",
    #         y=0.99,
    #         xanchor="right",
    #         x=0.99,
    #     )
    # )
    # layout = {
    #     "fig_bgcolor": "rgb(255, 255, 255)",
    #     "plot_bgcolor": "rgba(0, 0, 0, 0)",
    #     "paper_bgcolor": "rgba(0, 0, 0, 0)",
    # }

    # fig.update_layout(
    #     margin=dict(l=10, r=10, b=5, t=5, pad=4), bgcolor="rgb(255, 255, 255)"
    # )

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

    # fig.write_image(
    #     "images/{}_rate_{}{}.pdf".format(draw_type, tracker_type, element_type)
    # )
    # fig.show()
    # fig.write_image(
    #     "images/{}_rate_{}{}.pdf".format(draw_type, tracker_type, element_type)
    # )


# df_rate_company = pd.read_csv("dataset_archive/{}".format(name_dict["company"]))[:10]
# plot_figure12(df_rate_company, "company")

df_rate_tracker = pd.read_csv("dataset_archive/{}".format(name_dict["tracker"]))[:10]
print(df_rate_tracker)
plot_figure12(df_rate_tracker, "tracker")
