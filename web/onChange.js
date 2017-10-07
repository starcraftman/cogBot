function umChange(e) {
  // REQUIRED!!!! Force Google permission for active user, comes in event
  Session.getActiveUser()
  msg = {
    'spreadsheet': e.source.getName(),
    'sheet': e.source.getActiveSheet().getName(),
    'scanner': 'hudson_um',
    'time': new Date(),
  };

  var options = {
    'method' : 'post',
    'contentType': 'application/json',
    'payload' : JSON.stringify(msg),
    'validateHttpsCertificates': false
  };

  if (e.user.nickname != 'gearbot3003') {
    UrlFetchApp.fetch('starcraftman.com/post', options);
  }

  // Debug Only
  //e.range.setNote(JSON.stringify(msg));
}
