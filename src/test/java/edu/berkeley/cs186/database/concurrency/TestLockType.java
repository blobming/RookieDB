package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TimeoutScaling;
import edu.berkeley.cs186.database.categories.*;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

@Category({Proj4Tests.class, Proj4Part1Tests.class})
public class TestLockType {
    // 200ms per test
    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.millis((long) (
            200 * TimeoutScaling.factor)));

    /**
     * Compatibility Matrix
     * (Boolean value in cell answers is `left` compatible with `top`?)
     *
     *     | NL  | IS  | IX  |  S  | SIX |  X
     * ----+-----+-----+-----+-----+-----+-----
     * NL  |  T  |  T  |  T  |  T  |  T  |  T
     * ----+-----+-----+-----+-----+-----+-----
     * IS  |  T  |  T  |  T  |  T  |  T  |  F
     * ----+-----+-----+-----+-----+-----+-----
     * IX  |  T  |  T  |  T  |  F  |  F  |  F
     * ----+-----+-----+-----+-----+-----+-----
     * S   |  T  |  T  |  F  |  T  |  F  |  F
     * ----+-----+-----+-----+-----+-----+-----
     * SIX |  T  |  T  |  F  |  F  |  F  |  F
     * ----+-----+-----+-----+-----+-----+-----
     * X   |  T  |  F  |  F  |  F  |  F  |  F
     * ----+-----+-----+-----+-----+-----+-----
     */

    @Test
    @Category(PublicTests.class)
    public void testCompatibleNL() {
        // NL should be compatible with every lock type
        assertTrue(LockType.compatible(LockType.NL, LockType.NL));
        assertTrue(LockType.compatible(LockType.NL, LockType.S));
        assertTrue(LockType.compatible(LockType.NL, LockType.X));
        assertTrue(LockType.compatible(LockType.NL, LockType.IS));
        assertTrue(LockType.compatible(LockType.NL, LockType.IX));
        assertTrue(LockType.compatible(LockType.NL, LockType.SIX));
        assertTrue(LockType.compatible(LockType.S, LockType.NL));
        assertTrue(LockType.compatible(LockType.X, LockType.NL));
        assertTrue(LockType.compatible(LockType.IS, LockType.NL));
        assertTrue(LockType.compatible(LockType.IX, LockType.NL));
        assertTrue(LockType.compatible(LockType.SIX, LockType.NL));
    }

    @Test
    @Category(PublicTests.class)
    public void testCompatibleS() {
        // S is compatible with S, and IS
        assertTrue(LockType.compatible(LockType.S, LockType.S));
        assertTrue(LockType.compatible(LockType.S, LockType.IS));
        assertTrue(LockType.compatible(LockType.IS, LockType.S));

        // S is incompatible with X, IX, and SIX
        assertFalse(LockType.compatible(LockType.S, LockType.X));
        assertFalse(LockType.compatible(LockType.S, LockType.IX));
        assertFalse(LockType.compatible(LockType.S, LockType.SIX));
        assertFalse(LockType.compatible(LockType.X, LockType.S));
        assertFalse(LockType.compatible(LockType.IX, LockType.S));
        assertFalse(LockType.compatible(LockType.SIX, LockType.S));
    }

    @Test
    @Category(PublicTests.class)
    public void testCompatibleIntent() {
        // Intent locks are compatible with each other
        assertTrue(LockType.compatible(LockType.IS, LockType.IS));
        assertTrue(LockType.compatible(LockType.IS, LockType.IX));
        assertTrue(LockType.compatible(LockType.IX, LockType.IS));
        assertTrue(LockType.compatible(LockType.IX, LockType.IX));

        // SIX locks are compatible with IS locks only
        assertTrue(LockType.compatible(LockType.IS, LockType.SIX));
        assertTrue(LockType.compatible(LockType.SIX, LockType.IS));
        assertFalse(LockType.compatible(LockType.IX, LockType.SIX));
        assertFalse(LockType.compatible(LockType.SIX, LockType.IX));
        assertFalse(LockType.compatible(LockType.SIX, LockType.SIX));
    }

    @Test
    @Category(PublicTests.class)
    public void testCompatibleX() {
        // X locks are incompatible with X locks
        assertFalse(LockType.compatible(LockType.X, LockType.X));

        // X locks are incompatible with lock intents
        assertFalse(LockType.compatible(LockType.X, LockType.IS));
        assertFalse(LockType.compatible(LockType.X, LockType.IX));
        assertFalse(LockType.compatible(LockType.X, LockType.SIX));
        assertFalse(LockType.compatible(LockType.IS, LockType.X));
        assertFalse(LockType.compatible(LockType.IX, LockType.X));
        assertFalse(LockType.compatible(LockType.SIX, LockType.X));
    }

    @Test
    @Category(SystemTests.class)
    public void testParent() {
        // This is an exhaustive test of what we expect from LockType.parentLock
        // for valid lock types
        assertEquals(LockType.NL, LockType.parentLock(LockType.NL));
        assertEquals(LockType.IS, LockType.parentLock(LockType.S));
        assertEquals(LockType.IX, LockType.parentLock(LockType.X));
        assertEquals(LockType.IS, LockType.parentLock(LockType.IS));
        assertEquals(LockType.IX, LockType.parentLock(LockType.IX));
        assertEquals(LockType.IX, LockType.parentLock(LockType.SIX));
    }

    /**
     * Parent Matrix
     * (Boolean value in cell answers can `left` be the parent of `top`?)
     *
     *     | NL  | IS  | IX  |  S  | SIX |  X
     * ----+-----+-----+-----+-----+-----+-----
     * NL  |  T  |  F  |  F  |  F  |  F  |  F
     * ----+-----+-----+-----+-----+-----+-----
     * IS  |  T  |  T  |  F  |  T  |  F  |  F
     * ----+-----+-----+-----+-----+-----+-----
     * IX  |  T  |  T  |  T  |  T  |  T  |  T
     * ----+-----+-----+-----+-----+-----+-----
     * S   |  T  |  F  |  F  |  F  |  F  |  F
     * ----+-----+-----+-----+-----+-----+-----
     * SIX |  T  |  F  |  T  |  F  |  F  |  T
     * ----+-----+-----+-----+-----+-----+-----
     * X   |  T  |  F  |  F  |  F  |  F  |  F
     * ----+-----+-----+-----+-----+-----+-----
     */

    @Test
    @Category(PublicTests.class)
    public void testCanBeParentNL() {
        // Any lock type can be parent of NL
        for (LockType lockType : LockType.values()) {
            assertTrue(LockType.canBeParentLock(lockType, LockType.NL));
        }

        // The only lock type that can be a child of NL is NL
        for (LockType lockType : LockType.values()) {
            if (lockType != LockType.NL) {
                assertFalse(LockType.canBeParentLock(LockType.NL, lockType));
            }
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testIXParent() {
        // IX can be the parent of any type of lock
        for (LockType childType : LockType.values()) {
            assertTrue(LockType.canBeParentLock(LockType.IX, childType));
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testISParent() {
        // IS can be the parent of IS, S, and NL
        assertTrue(LockType.canBeParentLock(LockType.IS, LockType.IS));
        assertTrue(LockType.canBeParentLock(LockType.IS, LockType.S));
        assertTrue(LockType.canBeParentLock(LockType.IS, LockType.NL));

        // IS cannot be the parent of IX, X, or SIX
        assertFalse(LockType.canBeParentLock(LockType.IS, LockType.IX));
        assertFalse(LockType.canBeParentLock(LockType.IS, LockType.X));
        assertFalse(LockType.canBeParentLock(LockType.IS, LockType.SIX));
    }

    @Test
    @Category(PublicTests.class)
    public void testSIXParent() {
        // SIX can be the parent of X, IX
        assertTrue(LockType.canBeParentLock(LockType.SIX, LockType.X));
        assertTrue(LockType.canBeParentLock(LockType.SIX, LockType.IX));

        // SIX cannot be the parent of S, IS, SIX
        assertFalse(LockType.canBeParentLock(LockType.SIX, LockType.S));
        assertFalse(LockType.canBeParentLock(LockType.SIX, LockType.IS));
        assertFalse(LockType.canBeParentLock(LockType.SIX, LockType.SIX));
    }

    @Test
    @Category(PublicTests.class)
    public void testSParent() {
        // S cannot be the parent of any type of locks
        for (LockType lockType : LockType.values()) {
            if (lockType != LockType.NL) {
               assertFalse(LockType.canBeParentLock(LockType.S, lockType));
            }
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testXParent() {
        // X cannot be the parent of any type of locks
        for (LockType lockType : LockType.values()) {
            if (lockType != LockType.NL) {
               assertFalse(LockType.canBeParentLock(LockType.X, lockType));
            }
        }
    }

    /**
     * Substitutability Matrix
     * (The privileges of `left` are a superset of those of `top`)
     *
     *     | NL  | IS  | IX  |  S  | SIX |  X
     * ----+-----+-----+-----+-----+-----+-----
     * NL  |  T  |  F  |  F  |  F  |  F  |  F
     * ----+-----+-----+-----+-----+-----+-----
     * IS  |  T  |  T  |  F  |  F  |  F  |  F
     * ----+-----+-----+-----+-----+-----+-----
     * IX  |  T  |  T  |  T  |  F  |  F  |  F
     * ----+-----+-----+-----+-----+-----+-----
     * S   |  T  |  F  |  F  |  T  |  F  |  F
     * ----+-----+-----+-----+-----+-----+-----
     * SIX |  T  |  F  |  F  |  T  |  T  |  F
     * ----+-----+-----+-----+-----+-----+-----
     * X   |  T  |  F  |  F  |  T  |  F  |  T
     * ----+-----+-----+-----+-----+-----+-----
     */

    @Test
    @Category(PublicTests.class)
    public void testSubstitutableNL() {
        // You can't substitute anything with NL, other than NL
        assertTrue(LockType.substitutable(LockType.NL, LockType.NL));
        assertFalse(LockType.substitutable(LockType.NL, LockType.S));
        assertFalse(LockType.substitutable(LockType.NL, LockType.X));
        assertFalse(LockType.substitutable(LockType.NL, LockType.IS));
        assertFalse(LockType.substitutable(LockType.NL, LockType.IX));
        assertFalse(LockType.substitutable(LockType.NL, LockType.SIX));

        // You can substitute NL with anything
        assertTrue(LockType.substitutable(LockType.S, LockType.NL));
        assertTrue(LockType.substitutable(LockType.X, LockType.NL));
        assertTrue(LockType.substitutable(LockType.IS, LockType.NL));
        assertTrue(LockType.substitutable(LockType.IX, LockType.NL));
        assertTrue(LockType.substitutable(LockType.SIX, LockType.NL));
    }

    @Test
    @Category(PublicTests.class)
    public void testSubstitutableReal() {
        // You cannot substitute S with IS or IX
        assertFalse(LockType.substitutable(LockType.IS, LockType.S));
        assertFalse(LockType.substitutable(LockType.IX, LockType.S));

        // You can substitute S with S, SIX, or X
        assertTrue(LockType.substitutable(LockType.S, LockType.S));
        assertTrue(LockType.substitutable(LockType.SIX, LockType.S));
        assertTrue(LockType.substitutable(LockType.X, LockType.S));

        // You cannot substitute X with IS, IX, S, or SIX
        assertFalse(LockType.substitutable(LockType.IS, LockType.X));
        assertFalse(LockType.substitutable(LockType.IX, LockType.X));
        assertFalse(LockType.substitutable(LockType.S, LockType.X));
        assertFalse(LockType.substitutable(LockType.SIX, LockType.X));

        // You can substitute X with X
        assertTrue(LockType.substitutable(LockType.X, LockType.X));
    }

    @Test
    @Category(PublicTests.class)
    public void testSubstitutableIntent() {
        // You can substitute intent locks with themselves
        assertTrue(LockType.substitutable(LockType.IS, LockType.IS));
        assertTrue(LockType.substitutable(LockType.IX, LockType.IX));
        assertTrue(LockType.substitutable(LockType.SIX, LockType.SIX));

        // IX's privileges are a superset of IS's privileges
        assertTrue(LockType.substitutable(LockType.IX, LockType.IS));

        // IS's privileges are not a superset of IX's privileges
        assertFalse(LockType.substitutable(LockType.IS, LockType.IX));

        // IS's and IX's privileges are not a superset of SIX's privileges
        assertFalse(LockType.substitutable(LockType.IS, LockType.SIX));
        assertFalse(LockType.substitutable(LockType.IX, LockType.SIX));

        // You cannot substitute intent locks with real locks
        assertFalse(LockType.substitutable(LockType.S, LockType.IS));
        assertFalse(LockType.substitutable(LockType.X, LockType.IS));
        assertFalse(LockType.substitutable(LockType.S, LockType.IX));
        assertFalse(LockType.substitutable(LockType.X, LockType.IX));

        assertFalse(LockType.substitutable(LockType.S, LockType.SIX));
        assertFalse(LockType.substitutable(LockType.X, LockType.SIX));
        assertFalse(LockType.substitutable(LockType.SIX, LockType.IS));
        assertFalse(LockType.substitutable(LockType.SIX, LockType.IX));
    }

}

