# Qpid protonj2 Client Library

This client provides an imperative API for AMQP messaging applications

Below are some quick pointers you might find useful.

## Using the client library

To use the protonj2 API client library in your projects you can include the maven
dependency in your project pom file:

    <dependency>
      <groupId>org.apache.qpid</groupId>
      <artifactId>protonj2-client</artifactId>
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

## Client Documentation

The full client documentation is located in the Qpid protonj2 client [documentation](../protonj2-client-docs/README.md) module.