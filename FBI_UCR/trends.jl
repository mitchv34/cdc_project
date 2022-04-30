using CairoMakie
using Makie

f = CairoMakie.FileIO.Figure()

Axis(f[1, 1])

xs = 0:0.5:10
ys = sin.(xs)
lin = CairoMakie.lines!(xs, ys, color = :blue)
sca = CairoMakie.scatter!(xs, ys, color = :red, markersize = 15)


CairoMakie.Legend(f[2, 1], [lin, sca, lin], ["a line", "some dots", "line again"],
    orientation = :horizontal, tellwidth = false, tellheight = true)