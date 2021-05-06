# Running the client examples
----------------------------------------------

Use maven to build the module, and additionally copy the dependencies
alongside their output:

    mvn clean package dependency:copy-dependencies -DincludeScope=runtime -DskipTests

Now you can run the examples using commands of the format:

    Linux:   java -cp "target/classes/:target/dependency/*" org.apache.qpid.protonj2.client.examples.HelloWorld

    Windows: java -cp "target\classes\;target\dependency\*" org.apache.qpid.protonj2.client.examples.HelloWorld

NOTE: The examples expect to use a variety of addresses that are specific to one (or a pair of examples).
You may need to create these before running the examples, depending on the broker/peer you are using.

NOTE: By default the examples can only connect anonymously. A username and
password with which the connection can authenticate with the server may be set
through system properties named USER and PASSWORD respectively. E.g:

    Linux:   java -DUSER=guest -DPASSWORD=guest -cp "target/classes/:target/dependency/*" org.apache.qpid.protonj2.client.examples.HelloWorld

    Windows: java -DUSER=guest -DPASSWORD=guest -cp "target\classes\;target\dependency\*" org.apache.qpid.protonj2.client.examples.HelloWorld

By default the examples attempt to connect to a peer on the same machine however if the remote you are
attempting to test against is located on a different host or uses a non-standard AMQP port then the HOST
and PORT system properties can be provided to control where the examples will attempt their connections.

NOTE: The earlier build command will cause Maven to resolve the client artifact
dependencies against its local and remote repositories. If you wish to use a
locally-built client, ensure to "mvn install" it in your local repo first.
