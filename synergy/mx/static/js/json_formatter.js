/**
 * Taken from http://joncom.be/code/realtypeof/
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
