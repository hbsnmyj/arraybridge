#!/usr/bin/python
#
# BEGIN_COPYRIGHT
#
# Copyright (C) 2008-2015 SciDB, Inc.
# All Rights Reserved.
#
# SciDB is free software: you can redistribute it and/or modify
# it under the terms of the AFFERO GNU General Public License as published by
# the Free Software Foundation.
#
# SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
# INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
# NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
# the AFFERO GNU General Public License for the complete license terms.
#
# You should have received a copy of the AFFERO GNU General Public License
# along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
#
# END_COPYRIGHT
#
"""This file provides a routine to find Java8.

@author Donghui Zhang
"""
import os

__all__ = ['find']

def find():
    java8_candidate1 = "/usr/lib/jvm/java-1.8.0-openjdk/bin/java"
    java8_candidate2 = "/usr/lib/jvm/java-1.8.0-openjdk-amd64/bin/java"
    java8_candidate3 = "/usr/lib/jvm/java-1.8.0-openjdk.x86_64/bin/java"
    if os.path.exists(java8_candidate1):
        return java8_candidate1
    elif os.path.exists(java8_candidate2):
        return java8_candidate2
    elif os.path.exists(java8_candidate3):
        return java8_candidate3
    else:
        raise Exception("Cannot find Java 8.")
