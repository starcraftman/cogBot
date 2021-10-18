// Set this in the trigger management of an excel sheet.
// Use the onChange trigger and assign this function to handle the event.
// Update the 'scanner' line below depending on what scanner should schedule.
function onChange(e) {
  // REQUIRED!!!! Force Google permission for active user, comes in event
  Session.getActiveUser()
  msg = {
    //'spreadsheet': e.source.getName(),
    //'sheet': e.source.getActiveSheet().getName(),
    'time': new Date(),
    'scanner': 'hudson_um',  // Change this to name of scanner in config
  };

  // Make a POST the json to server
  var options = {
    'method' : 'post',
    'contentType': 'application/json',
    'payload' : JSON.stringify(msg),
    'validateHttpsCertificates': false
  };

  try {
    email = Session.getActiveUser().getEmail()
  } catch (err) {
    email = ""
  }

  // Ignore all changes by the actual bot account
  if (email.search("federalelitebot@cogent") === -1) {
    UrlFetchApp.fetch('starcraftman.com/post', options);
  }

}
