function onEdit(e) {
  msg = {
    'spreadsheet': e.source.getName(),
    'sheet': e.source.getActiveSheet().getName(),
    'range': e.range.getA1Notation(),
    'values': e.range.getValues(),
    'new_val': e.value,
    'old_val': e.oldValue,
    'time': new Date(),
  };

  // Make a POST the json to server
  var options = {
    'method' : 'post',
    'contentType': 'application/json',
    'payload' : JSON.stringify(msg),
    'validateHttpsCertificates': false
  };
  UrlFetchApp.fetch('starcraftman.com/post', options);

  // Debug Only
  //e.range.setNote(JSON.stringify(msg));
}
