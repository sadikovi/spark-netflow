/*
 * Copyright 2016 sadikovi
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.sadikovi.netflowlib.codegen;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class CodeGenContextSuite {
  // Dummy test node
  private CodeGenNode dummyNode = new CodeGenNode() {
    @Override
    public String nodeName(CodeGenContext ctx) {
      return "test";
    }

    @Override
    public String nodeType(CodeGenContext ctx) {
      return "java.lang.Object";
    }

    @Override
    public Class<?> nodeClass(CodeGenContext ctx) {
      return java.lang.Object.class;
    }
  };

  @Test
  public void testSingletonInstance() {
    CodeGenContext.reset();
    CodeGenContext instance1 = CodeGenContext.getOrCreate();
    CodeGenContext instance2 = CodeGenContext.getOrCreate();
    assertSame(instance1, instance2);
  }

  @Test
  public void testcanonicalize1() {
    CodeGenContext instance = CodeGenContext.getOrCreate();
    boolean fetchedException = false;
    try {
      instance.canonicalize(null);
    } catch (IllegalArgumentException iae) {
      fetchedException = true;
    }
    assertTrue(fetchedException);
  }

  @Test
  public void testcanonicalize2() {
    CodeGenContext instance = CodeGenContext.getOrCreate();
    boolean fetchedException = false;
    try {
      instance.canonicalize("");
    } catch (IllegalArgumentException iae) {
      fetchedException = true;
    }
    assertTrue(fetchedException);
  }

  @Test
  public void testcanonicalize3() {
    CodeGenContext instance = CodeGenContext.getOrCreate();
    assertEquals(instance.canonicalize("abc"), "abc");
    assertEquals(instance.canonicalize("aBc"), "aBc");
    assertEquals(instance.canonicalize("foo-bar"), "fooBar");
    assertEquals(instance.canonicalize("foo bar"), "fooBar");
    assertEquals(instance.canonicalize("foo_bar"), "fooBar");
    assertEquals(instance.canonicalize("foo?_bar"), "fooBar");
    assertEquals(instance.canonicalize("_foo(Bar)"), "fooBar");
    assertEquals(instance.canonicalize("_foo123"), "foo123");
    assertEquals(instance.canonicalize("foo-123"), "foo123");
    assertEquals(instance.canonicalize("123-foo"), "123Foo");
    assertEquals(instance.canonicalize("???"), CodeGenContext.DEFAULT_ATTRIBUTE);
  }

  @Test
  public void testInitRegisterNameState() {
    CodeGenContext.reset();
    CodeGenContext instance = CodeGenContext.getOrCreate();
    boolean done = instance.registerNameState(CodeGenContext.DEFAULT_ATTRIBUTE);
    assertEquals(done, false);
  }

  @Test
  public void testRegisterNameState() {
    CodeGenContext instance = CodeGenContext.getOrCreate();
    assertEquals(instance.registerNameState("abc"), true);
    assertEquals(instance.registerNameState("xyz"), true);
    // Another attempt to re-register should return false
    assertEquals(instance.registerNameState("abc"), false);
    assertEquals(instance.registerNameState("xyz"), false);
  }

  @Test
  public void testGetAttributeName1() {
    CodeGenContext.reset();
    CodeGenContext instance = CodeGenContext.getOrCreate();
    boolean fetchedException = false;
    try {
      instance.getAttributeName(null);
    } catch (IllegalArgumentException iae) {
      fetchedException = true;
    }
    assertTrue(fetchedException);
  }

  @Test
  public void testGetAttributeName2() {
    CodeGenContext.reset();
    CodeGenContext instance = CodeGenContext.getOrCreate();
    boolean fetchedException = false;
    try {
      // key that does not exist
      instance.getAttributeName("abc");
    } catch (IllegalArgumentException iae) {
      fetchedException = true;
    }
    assertTrue(fetchedException);
  }

  @Test
  public void testGetAttributeName3() {
    CodeGenContext.reset();
    CodeGenContext instance = CodeGenContext.getOrCreate();
    instance.registerNameState("abcd");
    assertEquals(instance.getAttributeName("abcd"), "abcd0");
    assertEquals(instance.getAttributeName("abcd"), "abcd1");
    assertEquals(instance.getAttributeName("abcd"), "abcd2");
  }

  @Test
  public void testGetAttributeName4() {
    CodeGenContext.reset();
    CodeGenContext instance = CodeGenContext.getOrCreate();
    instance.registerNameState("foo-bar");
    instance.registerNameState("fooBar");
    assertEquals(instance.getAttributeName("foo-bar"), "fooBar0");
    assertEquals(instance.getAttributeName("fooBar"), "fooBar1");
    assertEquals(instance.getAttributeName("foo-bar"), "fooBar2");
  }

  @Test
  public void testNormalizeJavaValues1() {
    CodeGenContext instance = CodeGenContext.getOrCreate();
    // check integer
    assertEquals(instance.normalizeJavaValue(10), "10");
    // check long
    assertEquals(instance.normalizeJavaValue(10L), "10L");
    // check byte
    assertEquals(instance.normalizeJavaValue((byte)10), "(byte) 10");
    // check short
    assertEquals(instance.normalizeJavaValue((short)10), "(short) 10");
  }

  // Do not support null references
  @Test
  public void testNormalizeJavaValues2() {
    CodeGenContext instance = CodeGenContext.getOrCreate();
    boolean fetchedException = false;
    try {
      instance.normalizeJavaValue(null);
    } catch (UnsupportedOperationException uoe) {
      fetchedException = true;
    }
    assertTrue(fetchedException);
  }

  // Do not support String values
  @Test
  public void testNormalizeJavaValues3() {
    CodeGenContext instance = CodeGenContext.getOrCreate();
    boolean fetchedException = false;
    try {
      instance.normalizeJavaValue("abc");
    } catch (UnsupportedOperationException uoe) {
      fetchedException = true;
    }
    assertTrue(fetchedException);
  }

  @Test
  public void testReset() {
    CodeGenContext instance = CodeGenContext.getOrCreate();
    // reset of non-null instance
    CodeGenContext.reset();
    // reset of null instance
    CodeGenContext.reset();
  }

  @Test
  public void testRegisterNode1() {
    CodeGenContext.reset();
    CodeGenContext instance = CodeGenContext.getOrCreate();
    boolean fetchedException = false;
    try {
      instance.registerNode(null, dummyNode);
    } catch (IllegalArgumentException uoe) {
      fetchedException = true;
    }
    assertTrue(fetchedException);
  }

  @Test
  public void testRegisterNode2() {
    CodeGenContext.reset();
    CodeGenContext instance = CodeGenContext.getOrCreate();
    boolean fetchedException = false;
    try {
      instance.registerNode("", dummyNode);
    } catch (IllegalArgumentException uoe) {
      fetchedException = true;
    }
    assertTrue(fetchedException);
  }

  @Test
  public void testRegisterNode3() {
    CodeGenContext.reset();
    CodeGenContext instance = CodeGenContext.getOrCreate();
    instance.registerNode("col", dummyNode);
    boolean fetchedException = false;
    try {
      instance.registerNode("col", dummyNode);
    } catch (IllegalStateException uoe) {
      fetchedException = true;
    }
    assertTrue(fetchedException);
  }

  @Test
  public void testGetNode1() {
    CodeGenContext.reset();
    CodeGenContext instance = CodeGenContext.getOrCreate();
    instance.registerNode("col", dummyNode);
    assertSame(instance.getNode("col"), dummyNode);
  }

  @Test
  public void testGetNode2() {
    CodeGenContext.reset();
    CodeGenContext instance = CodeGenContext.getOrCreate();
    boolean fetchedException = false;
    try {
      instance.getNode("col");
    } catch (IllegalStateException uoe) {
      fetchedException = true;
    }
    assertTrue(fetchedException);
  }
}
