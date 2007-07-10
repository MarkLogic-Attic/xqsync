#!/bin/sh
#
# sample bash script for running XQSync
#
# Copyright (c)2005-2007 Mark Logic Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# The use of the Apache License does not indicate that this project is
# affiliated with the Apache Software Foundation.
#

BASE=`readlink -f $0`
BASE=`dirname $BASE`
echo BASE=$BASE

CP=$HOME/lib/java/xcc.jar
CP=$CP:$HOME/lib/java/xstream-1.1.2.jar
CP=$CP:$BASE/../lib/xqsync.jar

FILES=
VMARGS=

for a in $*; do
    if [ -e "$a" ]; then
        FILES="$FILES $a"
    else
        VMARGS="$VMARGS $a"
    fi
done

if [ -d "$JAVA_HOME" ]; then
  JAVA=$JAVA_HOME/bin/java
else
  JAVA=java
fi

$JAVA -cp $CP $VMARGS com.marklogic.ps.xqsync.XQSync $FILES

# end xqsync.sh
