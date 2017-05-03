from dateutil.parser import parse
from datetime import datetime, date
from bokeh.plotting import figure, output_file, show

default_date = datetime.combine(date.today(), datetime.min.time()).replace(day=1)

inputFilePath = "new_addresses.txt"
data = [entry.strip("()\n").split(",") for entry in open(inputFilePath)]
data = [(parse(entry[0], default=default_date), entry[1]) for entry in data]
data = sorted(data, key=lambda x: x[0])

months = [entry[0] for entry in data]
counts = [entry[1] for entry in data]

output_file("new_addresses.html")
p = figure(title = "New Addresses Per Month", plot_width = 1100, x_axis_label = "Time", y_axis_label = "# of Addresses", x_axis_type = "datetime")
p.line(months, counts, line_width = 2)

show(p)
