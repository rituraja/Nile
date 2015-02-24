$(function() {
  $.getJSON('/api/getRealTime', function(data) {
    data.chart = {renderTo: "chart21", type: 'line', height: 500};
    data.plotOptions = {series: {colorByPoint: false}};
    $("#chart21").highcharts(data);
  });
  setInterval(function() {
    $.getJSON('/api/getRealTime', function(data) {
      data.chart = {renderTo: "chart21", type: 'line', height: 500};
      data.plotOptions = {series: {colorByPoint: false}};
      //$("#chart21").highcharts(data);
      $("#chart21").highcharts().series[0].setData(data.series[0].data, true);
    });
  }, 1000);
});
