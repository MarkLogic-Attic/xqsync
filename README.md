<!--
[![Download the latest version](https://api.bintray.com/packages/marklogic/maven/xqsync/images/download.svg)](https://bintray.com/marklogic/maven/xqsync/_latestVersion)
[![Javadocs](https://www.javadoc.io/badge/com.marklogic/xqsync.svg?color=blue)](https://www.javadoc.io/doc/com.marklogic/xqsync)
-->
[![Dependency Status](https://www.versioneye.com/user/projects/596a777a368b08005868208a/badge.svg?style=flat-square)](https://www.versioneye.com/user/projects/596a777a368b08005868208a)

[![Travis-ci gradle build status](https://travis-ci.org/marklogic-community/xqsync.svg?branch=develop)](https://travis-ci.org/marklogic-community/xqsync)
[![CircleCI maven build status](https://circleci.com/gh/marklogic-community/xqsync/tree/develop.svg?style=shield)](https://circleci.com/gh/marklogic-community/xqsync/tree/develop)
[![Codecov code coverage](https://codecov.io/gh/marklogic-community/xqsync/branch/develop/graph/badge.svg)](https://codecov.io/gh/marklogic-community/xqsync/branch/develop)
[![SonarQube TechDebt](https://sonarcloud.io/api/badges/measure?key=com.marklogic%3Axqsync%3Adevelop&metric=sqale_debt_ratio)](https://sonarcloud.io/component_measures/domain/Maintainability?id=com.marklogic%3Axqsync%3Adevelop)
[![SonarQube Quality](https://sonarcloud.io/api/badges/gate?key=com.marklogic%3Axqsync%3Adevelop)](https://sonarcloud.id/dashboard?id=com.marklogic%3Axqsync%3Adevelop)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/95ab38d35fbd467c9f31e12f2902d2d0)](https://www.codacy.com/app/mads-hansen/xqsync?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=marklogic-community/xqsync&amp;utm_campaign=Badge_Grade)


## XQSync: a Wheelbarrow for Content
MarkLogic Server includes built-in support for online, transactional backup and restore of both databases and forests. However, the on-disk format of these backups is platform-specific.

XQSync is an application-level synchronization tool that can copy documents and their metadata between MarkLogic databases. It can also package documents and their metadata as zip archives, or write them directly to a filesystem. XQSync can synchronize an entire database, a collection, a directory, or the results of evaluating an XQuery expression. Finally, XQSync can make some simple changes along the way: it can add a prefix or append a suffix to every document URI, and it can add new read permissions to every document.

Use XQSync when:
 - You want to copy a database between platforms.
 - You want to back up a portion of a database.

> Note: Starting with MarkLogic 6, [Marklogic Content Pump (mlcp)](https://developer.marklogic.com/products/mlcp), is a fully-supported tool that covers the same ground as this long-standing open source project. Content Pump is not supported on older versions of MarkLogic Server. Stick with XQSync if you are running earlier versions of MarkLogic.

### Running XQSync

XQSync is a Java command-line tool. The entry point is the main method in the `com.marklogic.ps.xqsync.XQSync` class. This class takes zero or more property files as its arguments. Any specified system properties will override file-based properties, and properties found in later files may override properties specified in earlier files on the command line. See src/xqsync.sh for a sample shell script.

```
java -cp xqsync.jar:xcc.jar:xstream.jar:xpp3.jar \
  com.marklogic.ps.xqsync.XQSync 
```

> Note: XQSync needs a lot of heap space for large synchronization tasks. Be prepared to increase the Java VM heap space limit, using -Xmx. Depending on the version of Java used, -Xincgc may also help.

#### Required properties:

 - one of: INPUT_PACKAGE, INPUT_CONNECTION_STRING
 - one of: OUTPUT_PACKAGE, OUTPUT_CONNECTION_STRING


> Note that this requirements can be overriden by a subclass of com.marklogic.ps.xqsync.Configuration. See Customization for details.

A full listing of [available properties](https://github.com/marklogic-community/xqsync/wiki/Properties).
 
### Getting Help
If you have a question, have an issue, or have a feature request:

* [Post a question to Stack Overflow](http://stackoverflow.com/questions/ask?tags=marklogic+xqsync) with the [<code>markogic</code>](https://stackoverflow.com/questions/tagged/marklogic) and [<code>xqsync</code>](https://stackoverflow.com/questions/tagged/xqsync) tags.  
* Submit issues or feature requests at https://github.com/marklogic-community/xqsync/issues

