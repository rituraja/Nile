$(function() {
  $.getJSON('/api/getDateTotalRevenue', function(data) {
    data.chart = {renderTo: "chart21", type: 'line', height: 500};
    data.plotOptions = {series: {colorByPoint: false}};
    $("#chart21").highcharts(data);
  });
});
