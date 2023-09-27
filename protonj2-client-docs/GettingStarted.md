# Getting started with the Qpid protonj2 Client Library

This client provides an imperative API for AMQP messaging applications

Below are some quick pointers you might find useful.

## Using the client library

To use the protonj2 API client library in your projects you can include the maven dependency in your project pom file:

    <dependency>
      <groupId>org.apache.qpid</groupId>
      <artifactId>protonj2-client</artifactId>
      <version>${protonj2-version}</version>
    </dependency>

## Building the code

The project requires Maven 3. Some example commands follow.

Clean previous builds output and install all modules to local repository without running the tests:

    mvn clean install -DskipTests

Install all modules to the local repository after running all the tests:

    mvn clean install

Perform a subset tests on the packaged release artifacts without installing:

    mvn clean verify -Dtest=TestNamePattern*

Execute the tests and produce code coverage report:

    mvn clean test jacoco:report

## Creating a connection

The entry point for creating new connections with the protonj2 client is the Client type which provides a simple static factory method to create new instances.

```
    Client container = Client.create();
```

The Client instance serves as a container for connections created by your application and can be used to close all active connections and provides the option of adding configuration to set the AMQP container Id that will be set on connections created from a given client instance.

Once you have created a Client instance you can use that to create new connections which will be of type ``Connection``. The Client instance provides API for creating a connection to a given host and port as well as providing connection options objects that carry a large set of connection specific configuration elements to customize the behavior of your connection. The basic create API looks as follows:

```
    Connection connection = container.connect(remoteAddress, remotePort, new ConnectionOptions());
```

From your connection instance you can then proceed to create sessions, senders and receivers that you can use in your application.

### Creating a message

The application code can create a message to be sent by using static factory methods in the ``Message`` type that can later be sent using the ``Sender`` type. These factory methods accept a few types that map nicely into the standard AMQP message format body sections.  A typical message creation example is shown below.

```
   final Message message = Message.create("message content");
```

The above code creates a new message object that carries a string value in the body of an AMQP message and it is carried inside an ``AmqpValue`` section. Other methods exist that wrap other types in the appropriate section types, a list of those is given below:

+ **Map** The factory method creates a message with the Map value wrapped in an ``AmqpValue`` section.
+ **List** The factory method creates a message with the List value wrapped in an ``AmqpSequence`` section.
+ **byte[]** The factory method creates a message with the byte array wrapped in an ``Data`` section.
+ **Object** All other objects are assumed to be types that should be wrapped in an ``AmqpValue`` section.

It is also possible to create an empty message and set a body and it will be wrapped in AMQP section types following the same rules as listed above. Advanced users should spend time reading the API documentation of the ``Message`` type to learn more.

### Sending a message

Once you have a connection you can create senders that can be used to send messages to a remote peer on a specified address. The connection instance provides methods for creating senders and is used as follows:

```
    Sender sender = connection.openSender("address");
```

A message instance must be created before you can send it and the Message interface provides simple static factory methods for common message types you might want to send, for this example we will create a message that carries text in an AmqpValue body section:

```
    Message<String> message = Message<String>.create("Hello World");
```

Once you have the message that you want to send the previously created sender can be used as follows:

```
    Tracker tracker = sender.send(message);
```

The Send method of a sender will attempt to send the specified message and if the connection is open and the send can be performed it will return a Tracker instance to provides API for checking if the remote has accepted the message or applied other AMQP outcomes to the sent message.

### Receiving a message

To receive a message sent to the remote peer a Receiver instance must be created that listens on a given address for new messages to arrive. The connection instance provides methods for creating receivers and is used as follows:

```
    Receiver receiver = connection.openReceiver("address");
```

After creating the receiver the application can then call one of the available receive APIs to await the arrival of a message from a remote sender.

```
    Delivery delivery = receiver.receive();
```

By default receivers created from the client API have a credit window configured and will manage the outstanding credit with the remote for your application however if you have configured the client not to manage a credit window then your application will need to provide receiver credit before invoking the receive APIs.

```
    receiver.addCredit(1);
```

Once a delivery arrives an Delivery instance is returned which provides API to both access the delivered message and to provide a disposition to the remote indicating if the delivered message is accepted or was rejected for some reason etc. The message is obtained by calling the message API as follows:

```
    Message<object> received = delivery.message();
```

Once the message is examined and processed the application can accept delivery by calling the accept method from the delivery object as follows:

```
    delivery.accept();
```

Other settlement options exist in the delivery API which provide the application wil full access to the AMQP specification delivery outcomes for the received message.

## Examples

First build and install all the modules as detailed above (if running against a source checkout/release, rather than against released binaries) and then consult the README.md in the protonj2-client-examples module itself.

