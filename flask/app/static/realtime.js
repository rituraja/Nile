$(function() {
  $.getJSON('/api/getRealTime', function(data) {
    data.chart = {renderTo: "chart21", type: 'area', height: 500};
    data.plotOptions = {series: {colorByPoint: false}};
    $("#chart21").highcharts(data);
  });
});
