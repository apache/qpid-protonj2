# Qpid protonj2 protocol engine

This library provides a protocol engine which can be used to build both clients and server that communicate over the AMQP v1.0 standard protocol.

Below are some quick pointers you might find useful.

## Using the protocol engine library

To use the protonj2 protocol engine library in your projects you can include the maven
dependency in your project pom file:

    <dependency>
      <groupId>org.apache.qpid</groupId>
      <artifactId>protonj2</artifactId>
      <version>${protonj2-version}</version>
    </dependency>

## Building the code

The project requires Maven 3. Some example commands follow.

Clean previous builds output and install all modules to local repository without
running the tests:

    mvn clean install -DskipTests

Install all modules to the local repository after running all the tests:

    mvn clean install

Perform a subset tests on the packaged release artifacts without
installing:

    mvn clean verify -Dtest=TestNamePattern*

Execute the tests and produce code coverage report:

    mvn clean test jacoco:report
