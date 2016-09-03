$(document).ready(function() {
    var pre = $('pre.code');
    if (pre.attr('date-url')) {
        var url = pre.attr('data-url');
        $.get(url).done(function(data) {
            var message = pre.html() + "\n" +
                          "******************************************\n" + 
                          "** Successfully loaded from: " + url + " **\n" +
                          data;
            pre.html(message);
        }).fail(function(xhr, status) {
            var message = pre.html() + "\n" +
                          "** Failed to fetch log from: " + url + " **\n" + 
                          "** Log server response code: " + status; 
            pre.html(message);
        });
    }
});