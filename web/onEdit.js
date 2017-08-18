function catEdit(e) {
  // REQUIRED!!!! Force Google permission for active user, comes in event
  Session.getActiveUser()
  msg = {
    'spreadsheet': e.source.getName(),
    'sheet': e.source.getActiveSheet().getName(),
    'range': e.range.getA1Notation(),
    'values': e.range.getValues(),
    'new_val': e.value,
    'old_val': e.oldValue,
    'user': e.user.nickname,
    'time': new Date(),
  };

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

function umChange(e) {
  // REQUIRED!!!! Force Google permission for active user, comes in event
  Session.getActiveUser()
  msg = {
    'spreadsheet': e.source.getName(),
    'sheet': e.source.getActiveSheet().getName(),
    'user': e.user.nickname,
    'time': new Date(),
    'change': true,
  };

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
