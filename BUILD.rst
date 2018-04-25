====
MMDS
====

Prerequisites
=============

- Java 8+ SDK
- Maven 3.1+
- Git

Build
=====
**Building with Maven**

- Clean all modules::

    mvn clean

- Run all tests, fail at the end::

    MAVEN_OPTS="-Xmx1024m -XX:MaxPermSize=128m" mvn test -fae

- Build all modules::

    mvn clean package

- Run checkstyle, skipping tests::

    mvn clean package -DskipTests

