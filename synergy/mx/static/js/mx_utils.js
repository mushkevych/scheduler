/* @author "Bohdan Mushkevych" */

/**
 * original source: underscorejs.org
 */
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

/**
 * original source: http://joncom.be/code/realtypeof/
 */
function realTypeOf(v) {
  if (typeof(v) == "object") {
    if (v === null) return "null";
    if (v.constructor == (new Array).constructor) return "array";
    if (v.constructor == (new Date).constructor) return "date";
    if (v.constructor == (new RegExp).constructor) return "regex";
    return "object";
  }
  return typeof(v);
}

/**
 * original source: http://joncom.be/code/realtypeof/
 */
function formatJSON(oData, sIndent) {
    if (arguments.length < 2) {
        sIndent = "";
    }
    var sIndentStyle = "    ";
    var sDataType = realTypeOf(oData);
    var iCount = 0;
    var sHTML;

    // open object
    if (sDataType == "array") {
        if (oData.length == 0) {
            return "[]";
        }
        sHTML = "[";
    } else {
        iCount = 0;
        $.each(oData, function() {
            iCount++;
        });
        if (iCount == 0) { // object is empty
            return "{}";
        }
        sHTML = "{";
    }

    // loop through items
    iCount = 0;
    $.each(oData, function(sKey, vValue) {
        if (iCount > 0) {
            sHTML += ",";
        }
        if (sDataType == "array") {
            sHTML += ("\n" + sIndent + sIndentStyle);
        } else {
            sHTML += ("\n" + sIndent + sIndentStyle + "\"" + sKey + "\"" + ": ");
        }

        // display relevant data type
        switch (realTypeOf(vValue)) {
            case "array":
            case "object":
                sHTML += formatJSON(vValue, (sIndent + sIndentStyle));
                break;
            case "boolean":
            case "number":
                sHTML += vValue.toString();
                break;
            case "null":
                sHTML += "null";
                break;
            case "string":
                sHTML += ("\"" + vValue + "\"");
                break;
            default:
                sHTML += ("TYPEOF: " + typeof(vValue));
        }

        // loop
        iCount++;
    });

    // close object
    if (sDataType == "array") {
        sHTML += ("\n" + sIndent + "]");
    } else {
        sHTML += ("\n" + sIndent + "}");
    }

    return sHTML;
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

/**
 * verifies if the given form_name::flag_name is set to *true*
 * triggers form submit if the flag is *false*
 * and converts all tables with style *synergy_datatable* to JS DataTable
 */
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

/**
 * creates an array of integers between two numbers, inclusive
 * @param start first value, inclusive
 * @param end last value, invclusive
 * @returns {Array}
 */
function range(start, end) {
    var myArray = [];
    for (var i = start; i <= end; i += 1) {
        myArray.push(i);
    }
    return myArray;
}
