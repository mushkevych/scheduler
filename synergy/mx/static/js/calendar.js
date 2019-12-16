// let d3 = require("d3@5");
// import {legend} from "@d3/color-legend";
// import "mx/static/js/calendar-data";

let cellSize = 17;
let width = 954;

const weekday = "monday";  // valid options: weekday, sunday, monday
let height = cellSize * (weekday === "weekday" ? 7 : 9);
let timeWeek = weekday === "sunday" ? d3.utcSunday : d3.utcMonday;
let countDay = weekday === "sunday" ? d => d.getUTCDay() : d => (d.getUTCDay() + 6) % 7;

function pathMonth(t) {
    const n = weekday === "weekday" ? 5 : 7;
    const d = Math.max(0, Math.min(n, countDay(t)));
    const w = timeWeek.count(d3.utcYear(t), t);
    return `${d === 0 ? `M${w * cellSize},0`
                      : d === n ? `M${(w + 1) * cellSize},0`
                                : `M${(w + 1) * cellSize},0V${d * cellSize}H${w * cellSize}`}V${n * cellSize}`;
}

let format = d3.format("+.2%");
let formatDate = d3.utcFormat("%x");
let formatDay = d => "SMTWTFS"[d.getUTCDay()];
let formatMonth = d3.utcFormat("%b");

let color_function = function () {
    const max = d3.quantile(data.map(d => Math.abs(d.value)).sort(d3.ascending), 0.995);

    // diverging, continuous color scale using the PifYG
    return d3.scaleSequential(d3.interpolatePiYG).domain([-max, +max]);
};

function color(value) {
    return color_function()(value);
}


let chart = function () {
    const years = d3.nest()
        .key(d => d.date.getUTCFullYear())
        .entries(data)
        .reverse();

    const svg = d3.select("svg")
        .attr("viewBox", [0, 0, width, height * years.length])
        .attr("font-family", "sans-serif")
        .attr("font-size", 10);

    const year = svg.selectAll("g")
        .data(years)
        .join("g")
        .attr("transform", (d, i) => `translate(40, ${height * i + cellSize * 1.5})`);

    year.append("text")
        .attr("x", -5)
        .attr("y", -5)
        .attr("font-weight", "bold")
        .attr("text-anchor", "end")
        .text(d => d.key);

    year.append("g")
        .attr("text-anchor", "end")
        .selectAll("text")
        .data((weekday === "weekday" ? d3.range(2, 7) : d3.range(7)).map(i => new Date(1995, 0, i)))
        .join("text")
        .attr("x", -5)
        .attr("y", d => (countDay(d) + 0.5) * cellSize)
        .attr("dy", "0.31em")
        .text(formatDay);

    year.append("g")
        .selectAll("rect")
        .data(d => d.values)
        .join("rect")
        .attr("width", cellSize - 1)
        .attr("height", cellSize - 1)
        .attr("x", d => timeWeek.count(d3.utcYear(d.date), d.date) * cellSize + 0.5)
        .attr("y", d => countDay(d.date) * cellSize + 0.5)
        .attr("fill", d => color(d.value))
        .append("title")
        .text(d => `${formatDate(d.date)}: ${format(d.value)}`);

    const month = year.append("g")
        .selectAll("g")
        .data(d => d3.utcMonths(d3.utcMonth(d.values[0].date), d.values[d.values.length - 1].date))
        .join("g");

    month.filter((d, i) => i).append("path")
        .attr("fill", "none")
        .attr("stroke", "#fff")
        .attr("stroke-width", 3)
        .attr("d", pathMonth);

    month.append("text")
        .attr("x", d => timeWeek.count(d3.utcYear(d), timeWeek.ceil(d)) * cellSize + 2)
        .attr("y", -5)
        .text(formatMonth);

    return svg.node();
};
