from dateutil.parser import parse
from bokeh.plotting import figure, output_file, show

inputFilePath = "new_addresses.txt"
data = [entry.strip("()\n").split(",") for entry in open(inputFilePath)]
data = [(entry[0], parse(entry[1]), entry[2]) for entry in data]
data = sorted(data, key=lambda x: x[1])

blocks = [entry[0] for entry in data]
times = [entry[1] for entry in data]
nums = [entry[2] for entry in data]

output_file("new_addresses.html")
p = figure(title = "New Addresses Per Block", x_axis_label = "Block", y_axis_label = "# of Addresses", x_axis_type = "datetime")
p.line(times, nums, line_width = 2)
show(p)
