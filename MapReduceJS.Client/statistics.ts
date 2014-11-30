/// <reference path="typings/google.visualization/google.visualization.d.ts" />
/// <reference path="typings/socket.io-client/socket.io-client.d.ts" />

var socket = io();

//socket.on('connect', log('Connected'));
//socket.on('connect_error', (data) => {
//	log('Connection failed' + data);s
//});

google.load("visualization", "1", { packages: ["corechart"] });
google.setOnLoadCallback(drawChart);

var data: google.visualization.DataTable = undefined;
var chart: google.visualization.ColumnChart = undefined;
var options = {
	title: 'MapReduceJS Statistics',
	hAxis: { title: '# of Workers' },
	animation: {
		duration: 200,
	},
};
var limit = 20;

function drawChart() {
	data = new google.visualization.DataTable();

	data.addColumn('datetime', 'Snapshot Time');
	data.addColumn('number', '# of Workers');

	chart = new google.visualization.AreaChart(document.getElementById('chart_div'));

	chart.draw(data, options);

	setInterval(() => socket.emit('stats'), 3000);
}

socket.on('stats', (stats) => {
	data.addRow([new Date(), stats]);

	if(data.getNumberOfRows() > limit) {
		data.removeRow(0);
	}

	chart.draw(data, options);
});
