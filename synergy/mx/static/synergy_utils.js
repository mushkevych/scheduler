/* @author "Bohdan Mushkevych" */

// debounce utility from underscorejs.org
function debounce(func, wait, immediate) {
    var timeout;
    return function () {
        var context = this, args = arguments;
        var later = function () {
            timeout = null;
            if (!immediate) func.apply(context, args);
        };
        if (immediate && !timeout) func.apply(context, args);
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}


function keys_to_list(dictionary, sorted) {
    var keys = [];
    for (var key in dictionary) {
        if (dictionary.hasOwnProperty(key)) {
            keys.push(key);
        }
    }

    if (sorted) {
        return keys.sort();
    } else {
        return keys;
    }
}


function get_url_parameter(sParam) {
    var sPageURL = window.location.search.substring(1);
    var sURLVariables = sPageURL.split('&');
    for (var i = 0; i < sURLVariables.length; i++) {
        var sParameterName = sURLVariables[i].split('=');
        if (sParameterName[0] == sParam) {
            return sParameterName[1];
        }
    }
    return null;
}

// verifies if the given form_name::flag_name is set to *true*
// triggers form submit if the flag is *false*
// and converts all tables with style *synergy_datatable* to JS DataTable
function load_dataset(form_name, flag_name, table_sorting) {
    if ($(form_name).data(flag_name) == false) {
        $(document).ready(function () {
            $(form_name).submit();
        });
    }

    // convert HTML table into JS dataTable
    $('.synergy_datatable').dataTable({"bPaginate": true,
        "bSort": true,
        "iDisplayLength": 36,
        "bLengthChange": false,
        "aaSorting": table_sorting
    });

}