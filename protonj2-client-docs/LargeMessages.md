# Sending and Receiving large messages with protonj2

When sending and receiving messages whose size exceeds what might otherwise be acceptable to having in memory all at once the protonj2 client has a flexible API to make this process simpler. The stream sender and stream receiver APIs in the protonj2 client offer the ability to read and write messages in manageable chunks that prevent application memory being exhausted trying to house the entire message. This API also provides a simple means of streaming files directly vs having to write a large amount of application code to perform such operations.

## Stream senders and receivers

The API for handling large message is broken out into stream senders and stream receivers that behave a bit differently than the stanard senders and receivers. Unlike the standard sender and receiver which operate on whole in memory messages the streaming API makes message content available through stream where bytes can be read or written in chunks without the need to have to entire contents in memory at once.  Also the underlying streaming implementation performs tight flow control to prevent the remote from sending to much before the local side has processed it, and sender blocks send operations when necessary to wait for capacity to send pending bytes to the remote.

## Using the stream sender

To send a large message using the stream sender API you need to create a ``StreamSender`` type which operates similar to the normal Sender API but imposes some restrictions on usage compared to the normal Sender.  Creating the stream sender is shown below:

```
   final StreamSender sender = connection.openStreamSender(address)
```

This code opens a new stream sender for the given address and returns a ``StreamSender`` type which you can then use to obtain the streaming message type which you will use to write the outgoing bytes. Unlike the standard message type the streaming message is created from the sender and it tied directly to that sender instance, and only one streaming message can be active on a sender at any give time. To create an outbound stream message use the following code:

```
   final StreamSenderMessage message = sender.beginMessage();
```

This requests that the sender initiate a new outbound streaming message and will throw an exception if another stream sender message is still active. The ``StreamSenderMessage`` is a specialized ``Message`` type whose body is an output stream type meaning that it behaves much like a normal message only the application must get a reference to the body output stream to write the outgoing bytes. The code below shows how this can be done in practice.

```
    message.durable(true);
    message.annotation("x-opt-annotation", "value");
    message.property("application-property", "value");

    // Creates an OutputStream that writes a single Data Section whose expected
    // size is configured in the stream options.
    final OutputStreamOptions streamOptions = new OutputStreamOptions().bodyLength(knownLength);
    final OutputStream output = message.body(streamOptions);

    while (<has data to send>) {
        output.write(buffer, i, chunkSize);
    }

    output.close();  // This completes the message send.

    message.tracker().awaitSettlement();
```

In the example above the application code has already obtained a stream sender message and uses it much like a normal message, setting application properties and annotations for the receiver to interpret and then begins writing from some data source a stream of bytes that will be encoded into an AMQP ``Data`` section as the body of the message, the sender will ensure that the writes occur in managable chunks and will not retain the previously written bytes in memory. A write call the the message body output stream can block if the sender is waiting on additional capacity to send or on IO level back-pressure to ease.

Once the application has written all the payload into the message it completes the operation by closing the ``OutputStream`` and then it can await settlement from the remote to indicate the message was received and processed successfully.

### Sending a large file using the stream sender

Sending a file using the ``StreamSenderMessage`` is an ideal use case for the stream sender. The first thing the application would need to do is to validate a file exists and open it (this is omitted here). Once a file has been opened the following code can be used to stream to contents to the remote peer.

```
    final File inputFile = <app opens file>;

    try (Connection connection = client.connect(serverHost, serverPort, options);
        StreamSender sender = connection.openStreamSender(address);
        FileInputStream inputStream = new FileInputStream(inputFile)) {

       final StreamSenderMessage message = sender.beginMessage();

       // Application can inform the other side what the original file name was.
       message.property(fileNameKey, inputFile.getName());

       try (OutputStream output = message.body()) {
           inputStream.transferTo(output);
       } catch (IOException e) {
           message.abort();
       }
```

In the example above the code makes use the JDK API from the ``InputStream`` class to transfer the contents of a file to the remote peer, the transfer API will read the contents of the file in small chunks and write those into the provided ``OutputStream`` which in this case is the stream from our ``StreamSenderMessage``.

## Using the stream receiver

To receive a large message using the stream receiver API you need to create a ``StreamReceiver`` type which operates similar to the normal Receiver API but imposes some restrictions on usage compared to the normal Sender.  Creating the stream receiver is shown below:

```
    final StreamReceiver receiver = connection.openStreamReceiver(address)) {
```

This code opens a new stream receiver for the given address and returns a ``StreamReceiver`` type which you can then use to obtain the streaming message type which you will use to read the incoming bytes. Just like the standard message type the streaming message is received from the receiver instance but and it is tied directly to that receiver as it read incoming bytes from the remote peer, therefore only one streaming message can be active on a receiver at any give time. To create an inbound stream message use the following code:

```
    final StreamDelivery delivery = receiver.receive();
    final StreamReceiverMessage message = delivery.message();
```

Once a new inbound streaming message has been received the application can read the bytes by requesting the ``InputStream`` from the message body and reading from it as one would any other input stream scenario.

```
    try (InputStream inputStream = message.body()) {
        final byte[] chunk = new byte[10];
        int readCount = 0;

        while (inputStream.read(chunk) != -1) {
            <Application logic to handle read bytes>
        }
    }
```

In the example code above the application reads from the message body input stream and simply writes out small chunks of the body to system out, the read calls might block while waiting for bytes to arrive from the remote but the application remains unaffected in this case.

### Receiving a large file using the stream receiver

Just as stream sending example from previously sent a large file using the ``StreamSenderMessage`` an application can receive and write a large message directly into a file with a few quick lines of code.  An example follows which shows how this can be done, the application is responsible for choosing a proper location for the file and verifiying that it has write access.

```
    try (Connection connection = client.connect(serverHost, serverPort, options);
         StreamReceiver receiver = connection.openStreamReceiver(address)) {

        StreamDelivery delivery = receiver.receive();
        StreamReceiverMessage message = delivery.message();

        // Application must choose a file name and check it can write to the
        // target location before receiving the contents.
        final String outputPath = ...
        final String filename = ...

        try (FileOutputStream outputStream = new FileOutputStream(new File(outputPath, filename))) {
            message.body().transferTo(outputStream);
        }

        delivery.accept();
    }
```

Just as in the stream sender case the application can make use of the JDK transfer API for ``InputStream`` instances to handle the bulk of the work reading small blocks of bytes and writing them into the target file, in most cases the application should add more error handling code not shown in the example. Reading from the incoming byte stream can block waiting for data from the remote which may need to be accounted for in some applications.

