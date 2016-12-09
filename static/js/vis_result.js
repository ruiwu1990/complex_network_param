$(document).ready(function(){
	$.get("/api/get_result",function(data){
		var inputJson = JSON.parse(data);
		var degree_distribution = inputJson['degree_distribution'];
		var chosen_closeness_cen = inputJson['chosen_closeness_cen'];
		var hop_distribution = inputJson['hop_distribution'];

		$('#closeness_centrality').text('closeness centrality of the chosen vertex is: '+closeness.toString());

		google.charts.load('current', {'packages':['corechart']});
	    google.charts.setOnLoadCallback(drawChart);

	    function drawChart() {

	    var degree_dist_arr = [['degree','count']];
	    var temp;
	    for(var i=0; i<degree_distribution.length; i++){
	    	temp = degree_distribution[i].split(',');
	    	degree_dist_arr.push([parseInt(temp[0]),parseInt(temp[1])]);
	    }

        var options1 = {
          title: 'Degree distribution',
          curveType: 'function',
          legend: { position: 'bottom' }
        };

        var hop_dist_arr = [['hop','count']];
		for(var i=0; i<hop_distribution.length; i++){
	    	hop_dist_arr.push([i,parseFloat(hop_distribution[i])]);
	    }

        var options2 = {
          title: 'Normalized hop distribution',
          curveType: 'function',
          legend: { position: 'bottom' }
        };

        var chart1 = new google.visualization.LineChart(document.getElementById('degree_distribution'));
        var chart2 = new google.visualization.LineChart(document.getElementById('hop_distribution'));

        chart1.draw(google.visualization.arrayToDataTable(degree_dist_arr), options);
        chart2.draw(data, options);
      }


	});
});