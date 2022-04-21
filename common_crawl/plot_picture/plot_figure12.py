"""
Author: your name
Date: 2022-01-07 11:15:34
LastEditTime: 2022-02-15 14:55:27
LastEditors: Please set LastEditors
Description: 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
FilePath: /common_crawl/resource/educational_websites_analyse/plot.py
"""

# 对实验结果进行绘画处理

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

draw_type = "company"

name_dict = {"company": "df_company_rate.csv", "tracker": "edu_trackers_rate.csv"}

df_rate = pd.read_csv(
    "resource/educational_websites_analyse/{}".format(name_dict[draw_type])
)[:10]
# print(df_rate)

# fig = make_subplots(rows=1, cols=2,subplot_titles=("The distribution of 3d-party's owner company ","The distribution of 3d-party"))


fig = go.Figure()
fig.add_trace(
    go.Bar(
        x=df_rate[draw_type],
        y=df_rate["edu"],
        # orientation='h',
        name="educational websites",
        marker_color="indianred",
        text=list(map(lambda x: round(x, 2), df_rate["edu"])),
    )
)

fig.add_trace(
    go.Bar(
        x=df_rate[draw_type],
        y=df_rate["no-edu"],
        # orientation='h',
        name="no-educational websites",
        marker_color="lightsalmon",
        text=list(map(lambda x: round(x, 2), df_rate["no-edu"])),
    )
)

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
fig.update_layout(
    barmode="group",
    xaxis_tickangle=-45,
    title_text="",
)
fig.update_layout(
    legend=dict(
        yanchor="top",
        y=0.99,
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

fig.write_image("images/{}_rate.pdf".format(draw_type))
fig.show()
fig.write_image("images/{}_rate.pdf".format(draw_type))
