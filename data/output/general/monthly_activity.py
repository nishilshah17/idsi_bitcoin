from dateutil.parser import parse
from datetime import datetime, date
from bokeh.io import gridplot
from bokeh.plotting import figure, output_file, show

default_date = datetime.combine(date.today(), datetime.min.time()).replace(day=1)

#parse input file
inputFilePath = "monthly_activity.txt"
data = [entry.strip("\n").split(",") for entry in open(inputFilePath)][1:]
data = [(parse(entry[0], dayfirst=False, yearfirst=True, default=default_date), entry[1], entry[2], entry[3]) for entry in data]
data = sorted(data, key=lambda x: x[0])

months = [entry[0] for entry in data]
blocks_count = [entry[1] for entry in data]
txs_count = [entry[2] for entry in data]
volume = [entry[3] for entry in data]

#plot
output_file("monthly_activity.html")
b = figure(title = "Blocks Per Month", plot_width = 1100, x_axis_label = "Time", y_axis_label = "# of Blocks", x_axis_type = "datetime")
b.line(months, blocks_count, line_width = 2, color='navy')

t = figure(title = "Transactions Per Month", plot_width = 1100, x_axis_label = "Time", y_axis_label = "# of Transactions", x_axis_type = "datetime")
t.line(months, txs_count, line_width = 2, color='firebrick')

v = figure(title = "Transaction Volume Per Month", plot_width = 1100, x_axis_label = "Time", y_axis_label = "Transaction Volume (in BTC)", x_axis_type = "datetime")
v.line(months, volume, line_width = 2, color='olive')

p = gridplot([b], [t], [v])
show(p)
