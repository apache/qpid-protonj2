# AMQPerative

Imperative API for AMQP messaging applications

Below are some quick pointers you might find useful.

## Using the AMQPerative library

To use the AMQPerative library in your projects you can include the maven
dependency in your project pom file:

    <dependency>
      <groupId>org.messaginghub.jms</groupId>
      <artifactId>amqperative-client</artifactId>
      <version>${amqperative-version}</version>
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

## Examples

First build and install all the modules as detailed above (if running against
a source checkout/release, rather than against released binaries) and then
consult the README in the AMQPerative-examples module itself.

## Documentation

There is some basic documentation in the amqperative-docs module.


