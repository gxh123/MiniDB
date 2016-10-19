/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.minidb.value;

//import org.h2.api.ErrorCode;
//import org.h2.message.DbException;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Implementation of the DECIMAL data type.
 */
public class ValueDecimal extends Value {

    /**
     * The value 'zero'.
     */
    public static final Object ZERO = new ValueDecimal(BigDecimal.ZERO);

    /**
     * The value 'one'.
     */
    public static final Object ONE = new ValueDecimal(BigDecimal.ONE);

    /**
     * The default precision for a decimal value.
     */
    static final int DEFAULT_PRECISION = 65535;

    /**
     * The default scale for a decimal value.
     */
    static final int DEFAULT_SCALE = 32767;

    /**
     * The default display size for a decimal value.
     */
    static final int DEFAULT_DISPLAY_SIZE = 65535;

    private static final int DIVIDE_SCALE_ADD = 25;

    /**
     * The maximum scale of a BigDecimal value.
     */
    private static final int BIG_DECIMAL_SCALE_MAX = 100000;

    private final BigDecimal value;
    private String valueString;
    private int precision;

    private ValueDecimal(BigDecimal value) {
        if (value == null) {
            throw new IllegalArgumentException("null");
        } else if (!value.getClass().equals(BigDecimal.class)) {
            throw new RuntimeException("ValueDecimal ERROR");
        }
        this.value = value;
    }

    public String getSQL() {
        return getString();
    }

    @Override
    public String getString() {
        if (valueString == null) {
            String p = value.toPlainString();
            if (p.length() < 40) {
                valueString = p;
            } else {
                valueString = value.toString();
            }
        }
        return valueString;
    }

    @Override
    public int getType() {
        throw new RuntimeException("getType ERROR");
//        return 0;
    }

    public int hashCode() {
        return value.hashCode();
    }

    public Object getObject() {
        return value;
    }

    public void set(PreparedStatement prep, int parameterIndex)
            throws SQLException {
        prep.setBigDecimal(parameterIndex, value);
    }

    /**
     * Get or create big decimal value for the given big decimal.
     *
     * @param dec the bit decimal
     * @return the value
     */
    public static ValueDecimal get(BigDecimal dec) {
        if (BigDecimal.ZERO.equals(dec)) {
            return (ValueDecimal) ZERO;
        } else if (BigDecimal.ONE.equals(dec)) {
            return (ValueDecimal) ONE;
        }
        return (ValueDecimal) new ValueDecimal(dec);
    }

    @Override
    public boolean equals(Object other) {
        // Two BigDecimal objects are considered equal only if they are equal in
        // value and scale (thus 2.0 is not equal to 2.00 when using equals;
        // however -0.0 and 0.0 are). Can not use compareTo because 2.0 and 2.00
        // have different hash codes
        return other instanceof ValueDecimal &&
                value.equals(((ValueDecimal) other).value);
    }

    public int getMemory() {
        return value.precision() + 120;
    }

    @Override
    protected int compareSecure(Value o) {
        ValueDecimal v = (ValueDecimal) o;
        return value.compareTo(v.value);
    }

}
