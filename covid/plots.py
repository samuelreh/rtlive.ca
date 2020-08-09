import numpy as np
import plotly
import plotly.graph_objects as go


def rt(model, result):
    samples = model.trace["r_t"]
    x = result.index
    x_rev = x[::-1]
    samples = samples.T

    upper = np.percentile(samples, 99, axis=1)
    upper_greater_than_1 = [u if u > 1 else 1 for u in upper]
    upper_less_than_1 = [u if u < 1 else 1 for u in upper]
    lower = np.percentile(samples, 1, axis=1)
    lower_greater_than_1 = [l if l > 1 else 1 for l in lower]
    lower_less_than_1 = [l if l < 1 else 1 for l in lower]
    one = np.full(len(upper), 1)

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=list(x) + list(x_rev),
            y=list(upper_greater_than_1) + list(lower_greater_than_1[::-1]),
            fill="toself",
            fillcolor="rgba(150,0,0,0.2)",
            line_color="rgba(0,0,0,0)",
            showlegend=False,
        )
    )

    fig.add_trace(
        go.Scatter(
            x=list(x) + list(x_rev),
            y=list(lower_less_than_1) + list(upper_less_than_1[::-1]),
            fill="toself",
            fillcolor="rgba(0,150,0,0.2)",
            line_color="rgba(0,0,0,0)",
            showlegend=False,
        )
    )

    # Red Median > 0
    fig.add_trace(
        go.Scatter(
            x=x,
            y=result["median"],
            line_color="rgb(150,0,0,1)",
            name="Rt",
            showlegend=False,
        )
    )

    # Green Median < 0
    fig.add_trace(
        go.Scatter(
            x=x,
            y=result["median"].where(result["median"] <= 1),
            line_color="rgb(0,150,0,1)",
            showlegend=False,
            hoverinfo="none",
        )
    )

    fig.update_layout(
        template="plotly_white",
        hovermode="x unified",
        margin=dict(r=0, l=0, t=40),
        xaxis=dict(fixedrange=True),
        yaxis=dict(fixedrange=True),
    )
    fig.show(config={"displayModeBar": False})


def positives(result):
    data = result[result.test_adjusted_positive > 0]

    fig = go.Figure()
    fig.update_layout(
        template="plotly_white",
        hovermode="x unified",
        margin=dict(r=0, l=0, t=40),
        xaxis=dict(fixedrange=True),
        yaxis=dict(fixedrange=True),
        legend=dict(yanchor="top", xanchor="right"),
    )
    fig.add_trace(
        go.Bar(
            x=data.index,
            y=data.positive,
            marker_color="rgba(150, 0, 0, .4)",
            name="Reported data",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=data.index,
            y=data.test_adjusted_positive,
            mode="lines",
            marker_color="rgba(0, 0, 0, .4)",
            name="Test-adjusted data",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=data.index,
            y=data.infections,
            mode="lines",
            name="Implied Infections",
            marker_color="rgba(0, 145, 255, 0.7)",
        )
    )
    fig.show(config={"displayModeBar": False})


def tests(result):
    data = result[result.test_adjusted_positive > 0]
    fig = go.Figure()
    fig.update_layout(
        template="plotly_white",
        hovermode="x",
        margin=dict(r=0, l=0, t=40),
        xaxis=dict(fixedrange=True),
        yaxis=dict(fixedrange=True),
    )
    fig.add_trace(
        go.Bar(x=data.index, y=data.tests, marker_color="rgba(26, 118, 255, .4)")
    )
    fig.show(config={"displayModeBar": False})
