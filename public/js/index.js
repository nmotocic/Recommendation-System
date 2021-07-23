const width = 1600;
const height = 800;

var links;
var nodes;

var simulation;
var transform;

var canvas = d3.select("canvas")
var context = canvas.node().getContext('2d')

var xmlhttp = new XMLHttpRequest();

var radius = 5;

get_graph()

function get_graph(){
    xmlhttp.open("GET", "/load-all", true);
    xmlhttp.setRequestHeader('Content-type', 'application/json; charset=utf-8');

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
    book_id = find_book(event);

    if(book_id != null){
        xmlhttp.open("GET", "/book-properties" + "/" + book_id.toString(), true);
        xmlhttp.setRequestHeader("Content-type", "application/json; charset=utf-8");

        xmlhttp.onreadystatechange = function() {
            if(xmlhttp.readyState == 4 && xmlhttp.status == "200"){
                data = JSON.parse(xmlhttp.responseText);
                var properties = data.properties;

                d3.select("#tooltip")
                  .style("visibility", "visible")
                  .style("opacity", 0.8)
                  .style("top", event.y + 'px')
                  .style("left", event.x + 'px')
                  .html(
                      "ISBN: " + properties["ISBN"] + "<br>" +
                      "Title: " + properties["title"] + "<br>" +
                      "Author: " + properties["author"] + "<br>" +
                      "Year of publishing: " + properties["yop"] + "<br>" +
                      "Language: " + properties["lang"] + "<br>" +
                      "Summary: " + properties["summary"]
                  );
            }
        }
        xmlhttp.send();
    } else {
        d3.select("#tooltip")
          .style("visiblity", "hidden");
    }
}

function find_book(event){
    var pos = get_mouse_pos(event);
    var result = null
    nodes.forEach(function (d, i) {
        if (Math.sqrt(Math.pow((d.new_x - pos.x), 2) + Math.pow((d.new_y - pos.y), 2)) < radius * transform.k){
            result = d.id
        }
    });
    return result;
}

function dragsubject(event) {
    var i,
        x = transform.invertX(event.x),
        y = transform.invertY(event.y),
        dx,
        dy;
    for (i = nodes.length - 1; i >= 0; --i) {
        node = nodes[i];
        dx = x - node.x;
        dy = y - node.y;

        if (dx * dx + dy * dy < radius * radius) {
            node.x = transform.applyX(node.x);
            node.y = transform.applyY(node.y);
            return node;
        }
    }
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
        context.moveTo(d.source.x, d.source.y);
        context.lineTo(d.target.x, d.target.y);
        context.stroke();
    });
    nodes.forEach(function (d, i) {
        context.beginPath();
        context.arc(d.x, d.y, radius, 0, 2 * Math.PI, true);
        context.fillStyle = "#FFA500";
        context.fill();
    });
    
    context.restore();
}