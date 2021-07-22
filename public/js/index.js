const width = window.innerWidth, height = window.innerHeight;

var links;
var nodes;

var simulation;
var transformation;

var canvas = d3.select("canvas")
var context = canvas.node().getContext('2d')

var xmlhttp = new XMLHttpRequest();

get_graph()

function get_graph(){
    xmlhttp.open("GET", "/load-all", true);
    xmlhttp.setRequestHeader('Content-type', 'application/json; charset)utf-8');

    xmlhttp.onreadystatechange = function () {
        if (xmlhttp.readyState == 4 && xmlhttp.status == "200"){
            data = JSON.parse(xmlhttp.responseText);
            links = data.links;
            nodes = data.nodes;

            simulation = d3.forceSimulation(nodes)
                        .force("center", d3.forceCenter (width / 2, height / 2))
                        .force("charge", d3.forceManyBody().strength(-50))
                        .force("link", d3.forceLink().strength(0.1).id(function (d) {return d.id; }))
                        .alphaTarget(0)
                        .alphaDecay(0.05);

            transform = d3.zoomIdentity;

            d3.select(context.canvas)
            .call(d3.drag().subject(dragsubject).on("start", dragstarted).on("drag", dragged).on("end", dragended))
            .call(d3.zoom().scaleExtend([1/10, 8]).on("zoom", zoomed));

            d3.select(context.canvas)
            .on("click", function(d) { show_properties(d); });
            
            simulation.nodes(nodes).on("tick", simulationUpdate);
            simulation.force("link").links(links);
        }
    }
    xmlhttp.send();   
}

function show_properties(event) {
    //TODO: Properties
}

function dragstarted(event) {
    if (!event.active) simulation.alphaTarget(0.3).restart();
    event.subject.fx = transform.invertX(event.x);
    event.subject.fy = transform.invertY(event.y);
}


function dragged(event) {
    event.subject.fx = transform.invertX(event.x);
    event.subject.fy = transform.invertY(event.y);
}


function dragended(event) {
    if (!event.active) simulation.alphaTarget(0);
    event.subject.fx = null;
    event.subject.fy = null;
}

function get_mouse_pos(evt) {
    var rect = canvas.node().getBoundingClientRect();
    return {
      x: evt.clientX - rect.left,
      y: evt.clientY - rect.top
    };
}

function simulationUpdate() {
    context.save();
    context.clearRect(0, 0, width, height);
    context.translate(transform.x, transform.y);
    context.scale(transform.k, transform.k);

    links.forEach(function (d) {
        context.beginPath();

        context.globalAlpha = alpha(d);

        context.moveTo(d.source.x, d.source.y);
        context.lineTo(d.target.x, d.target.y);

        context.strokeStyle = "#7ecd8a";
        context.stroke();

        context.globalAlpha = 1;
    });
    nodes.forEach(function (d, i) {
        context.beginPath();

        context.shadowColor = " #7ecd8a";
        context.shadowBlur = 10 + d.bc * 1000;

        context.fillStyle = "#5abf69";
        context.arc(d.x, d.y, 50, 0, 2 * Math.PI, true);
        context.fill();

        context.strokeStyle = "#245c2c";

        context.font = "5px Comic Sans MS";
        context.fillStyle = "#15371a";
        context.textAlign = "center";
        context.fillText(d.symbol, d.x, d.y + 50 + 5);

        context.stroke();
        
        d.new_x = (d.x * transform.k + transform.x);
        d.new_y = (d.y * transform.k + transform.y);
    });
    
    context.restore();
}