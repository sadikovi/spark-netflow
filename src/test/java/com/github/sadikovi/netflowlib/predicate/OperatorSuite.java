package com.github.sadikovi.netflowlib.predicate;

import java.util.HashSet;

import org.junit.Test;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.github.sadikovi.netflowlib.predicate.Columns.IntColumn;
import com.github.sadikovi.netflowlib.predicate.Columns.ShortColumn;
import com.github.sadikovi.netflowlib.predicate.FilterApi;
import com.github.sadikovi.netflowlib.predicate.Operators.Eq;
import com.github.sadikovi.netflowlib.predicate.Operators.Gt;
import com.github.sadikovi.netflowlib.predicate.Operators.In;
import com.github.sadikovi.netflowlib.predicate.Operators.And;

public class OperatorSuite {
  @Test
  public void testEquality() {
    IntColumn col = new IntColumn("col1", 0);

    Eq<Integer> pr1 = FilterApi.eq(col, 10);
    assertTrue(pr1.equals(pr1));

    Eq<Integer> pr2 = FilterApi.eq(col, 10);
    assertTrue(pr1.equals(pr2));

    Gt<Integer> pr3 = FilterApi.gt(col, 10);
    assertFalse(pr1.equals(pr3));

    HashSet<Integer> set = new HashSet<Integer>();
    set.add(10);
    In<Integer> pr4 = FilterApi.in(col, set);
    assertFalse(pr1.equals(pr4));

    And pr5 = FilterApi.and(pr1, pr2);
    assertFalse(pr1.equals(pr5));
  }
}
