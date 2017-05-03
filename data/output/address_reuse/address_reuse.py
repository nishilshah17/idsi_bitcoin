from bokeh.plotting import figure, output_file, show

inputFilePath = "address_reuse_count.txt"
data = [entry.strip("()\n").split(",") for entry in open(inputFilePath)]
data = [(int(entry[0]), int(entry[1])) for entry in data]
data = sorted(data, key=lambda x: x[0])

print(data)

reuse_times = [entry[0] for entry in data]
address_count = [entry[1] for entry in data]

output_file("address_reuse_times.html")
p = figure(title = "Number of Addresses Reused", x_axis_label = "Times Reused", y_axis_label = "# of Addresses")
p.vbar(reuse_times, 0.5, 0, address_count, color = "firebrick")
show(p)
