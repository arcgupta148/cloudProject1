var checkout = {};

$(document).ready(function() {
  var $messages = $('.messages-content'),
    d, h, m,
    i = 0;

  $(window).load(function() {
    $messages.mCustomScrollbar();
    insertResponseMessage('Hi there, I\'m your personal Concierge. How can I help?');
  });

  function updateScrollbar() {
    $messages.mCustomScrollbar("update").mCustomScrollbar('scrollTo', 'bottom', {
      scrollInertia: 10,
      timeout: 0
    });
  }

  function setDate() {
    d = new Date()
    if (m != d.getMinutes()) {
      m = d.getMinutes();
      $('<div class="timestamp">' + d.getHours() + ':' + m + '</div>').appendTo($('.message:last'));
    }
  }

  function callChatbotApi(message) {
    // params, body, additionalParams
    return sdk.chatbotPost({}, {
      messages: [{
        type: 'unstructured',
        unstructured: {
          text: message
        }
      }]
    }, {});
  }

  function insertMessage() {
    msg = $('.message-input').val();
    if ($.trim(msg) == '') {
      return false;
    }
    $('<div class="message message-personal">' + msg + '</div>').appendTo($('.mCSB_container')).addClass('new');
    setDate();
    $('.message-input').val(null);
    updateScrollbar();

  //   callChatbotApi(msg)
  //     .then((response) => {
  //       //console.log(response);
  //       var data = response.data;
  //       //console.log(data);
  //       if (data.messages && data.messages.length > 0) {
  //         console.log('received ' + data.messages.length + ' messages');

  //         var messages = data.message;
  //         console.log('message is' + messages);
  //         for (var message of messages) {
  //           if (message.type === 'unstructured') {
  //             insertResponseMessage(message.unstructured.text);
  //           } else if (message.type === 'structured' && message.structured.type === 'product') {
  //             var html = '';

  //             insertResponseMessage(message.structured.text);

  //             setTimeout(function() {
  //               html = '<img src="' + message.structured.payload.imageUrl + '" witdth="200" height="240" class="thumbnail" /><b>' +
  //                 message.structured.payload.name + '<br>$' +
  //                 message.structured.payload.price +
  //                 '</b><br><a href="#" onclick="' + message.structured.payload.clickAction + '()">' +
  //                 message.structured.payload.buttonLabel + '</a>';
  //               insertResponseMessage(html);
  //             }, 1100);
  //           } else {
  //             console.log('not implemented');
  //           }
  //         }
  //       } else {
  //         insertResponseMessage('Oops, something went wrong. Please try again.');
  //       }
  //     })
  //     .catch((error) => {
  //       console.log('an error occurred', error);
  //       insertResponseMessage('Oops, something went wrong. Please try again.');
  //     });
  // }
  callChatbotApi(msg)
  .then((response) => {
    // Debugging: Log the full response received from the API
    console.log('API response received:', response);

    // Extract the data from the response
    var data = response.data;
    console.log('Extracted data:', data);

    // Check if the body field exists and needs to be parsed
    if (data.body) {
      try {
        // Debugging: Log the raw body before parsing
        console.log('Raw body before parsing:', data.body);

        // Parse the body if it's a JSON string
        data = JSON.parse(data.body);

        // Debugging: Log the parsed body
        console.log('Parsed body:', data);
      } catch (e) {
        // Debugging: Log an error if parsing fails
        console.error('Error parsing the body as JSON. Treating as plain text:', e);

        // Insert the plain text message into the chat
        insertResponseMessage(data.body);

        // Exit since we handled the plain text case
        return;
      }
    }

    // Debugging: Log the final data after potential parsing
    console.log('Final data after handling body:', data);
    if (typeof data.message === 'string') {
      console.log('Received message from API as it is string is:', data.message);
      insertResponseMessage(data.message);
    }

    // Check if messages exist in the data
    if (data.message && data.message.length > 0) {
      console.log('Received ' + data.message.length + ' message(s) from API');

      // Handle each message in the messages array
      var messages = data.message;
      console.log('Messages array:', message);

      for (var message of messages) {
        console.log('Processing message:', message);

        if (message.type === 'unstructured') {
          // Debugging: Log the unstructured message
          console.log('Inserting unstructured message:', message.unstructured.text);

          // Insert unstructured message (plain text)
          insertResponseMessage(message.unstructured.text);

        } else if (message.type === 'structured' && message.structured.type === 'product') {
          // Debugging: Log the structured message
          console.log('Inserting structured message:', message.structured);

          insertResponseMessage(message.structured.text);

          // Delayed insertion of structured product details
          setTimeout(function() {
            var html = '<img src="' + message.structured.payload.imageUrl + '" width="200" height="240" class="thumbnail" />' +
              '<b>' + message.structured.payload.name + '<br>$' + message.structured.payload.price + '</b><br>' +
              '<a href="#" onclick="' + message.structured.payload.clickAction + '()">' + message.structured.payload.buttonLabel + '</a>';

            // Debugging: Log the constructed HTML for the structured message
            console.log('Inserting structured HTML:', html);

            insertResponseMessage(html);
          }, 1100);
        } else {
          // Debugging: Log message type not implemented
          console.log('Message type not implemented. Type:', message.type);
        }
      }
    } else {
      // Debugging: Log when no messages array is found or it's empty
      console.warn('No messages found in API response or messages array is empty.');
      insertResponseMessage(data);
      //insertResponseMessage('Oops, something went wrong. Please try again.');
    }
  })
  .catch((error) => {
    // Debugging: Log any errors that occur during the API call or response handling
    console.error('An error occurred during the API call:', error);

    insertResponseMessage('Oops, something went wrong. Please try again.');
  });
  }
  $('.message-submit').click(function() {
    insertMessage();
  });

  $(window).on('keydown', function(e) {
    if (e.which == 13) {
      insertMessage();
      return false;
    }
  })

  function insertResponseMessage(content) {
    $('<div class="message loading new"><figure class="avatar"><img src="https://media.tenor.com/images/4c347ea7198af12fd0a66790515f958f/tenor.gif" /></figure><span></span></div>').appendTo($('.mCSB_container'));
    updateScrollbar();

    setTimeout(function() {
      $('.message.loading').remove();
      $('<div class="message new"><figure class="avatar"><img src="https://media.tenor.com/images/4c347ea7198af12fd0a66790515f958f/tenor.gif" /></figure>' + content + '</div>').appendTo($('.mCSB_container')).addClass('new');
      setDate();
      updateScrollbar();
      i++;
    }, 500);
  }

});
