/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.common.util;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

public class TestObjectSizeCalculator {

  static class Dummy {

    int a = 80;
    long b = 80000000L;
    String c = "aaaaaaaaaaaaaaaaa";
  }

  static class Dummy2 extends Dummy {

    transient String d = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
  }


  @Test
  public void compareSizes() throws UnsupportedOperationException {

    List<Object> objects = Arrays.asList(
        "aaa",
        "aaaaaaaaaa",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        new Dummy(),
        new Dummy2()
    );

    for (Object o : objects) {
      long v1 = ObjectSizeCalculator.getObjectSizePrimary(o);
      long v2 = ObjectSizeCalculator.getObjectSizeFallback(o);
      System.out.println(v1 + ":" + v2);
    }
  }
}
